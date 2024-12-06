use std::{marker::PhantomData, ops::AddAssign};

use async_trait::async_trait;
use ceramic_actor::{actor_envelope, Actor, ActorRef, Handler, Message, Receiver, Sender};
use tracing::{instrument, Instrument, Level};

struct Game {
    scores: Scores,
    receiver: Option<Receiver<<Game as Actor>::Envelope>>,
}

impl Actor for Game {
    type Envelope = GameEnvelope;

    fn receiver(&mut self) -> Receiver<Self::Envelope> {
        self.receiver.take().unwrap()
    }
}

actor_envelope! {
    GameEnvelope,
    GetScore => GetScoreMessage,
    Score => ScoreMessage,
}

#[derive(Debug)]
struct ScoreMessage {
    scores: Scores,
}
impl Message for ScoreMessage {
    type Result = ();
}

#[derive(Debug)]
struct GetScoreMessage;
impl Message for GetScoreMessage {
    type Result = Scores;
}
#[derive(Clone, Debug)]
struct Scores {
    home: usize,
    away: usize,
}
impl AddAssign for Scores {
    fn add_assign(&mut self, rhs: Self) {
        self.home += rhs.home;
        self.away += rhs.away;
    }
}

#[async_trait]
impl Handler<ScoreMessage> for Game {
    #[instrument(skip(self), ret(level = Level::DEBUG))]
    async fn handle(&mut self, message: ScoreMessage) -> <ScoreMessage as Message>::Result {
        self.scores += message.scores;
    }
}
#[async_trait]
impl Handler<GetScoreMessage> for Game {
    #[instrument(skip(self), ret(level = Level::DEBUG))]
    async fn handle(&mut self, _message: GetScoreMessage) -> <GetScoreMessage as Message>::Result {
        self.scores.clone()
    }
}

#[derive(Clone)]
struct GameRef {
    sender: Sender<<Game as Actor>::Envelope>,
}

#[async_trait]
impl ActorRef<Game> for GameRef {
    fn sender(&self) -> Sender<<Game as Actor>::Envelope> {
        self.sender.clone()
    }
}

impl GameRef {
    pub fn new() -> Self {
        let (sender, receiver) = ceramic_actor::channel(8);
        let actor = Game {
            scores: Scores { home: 0, away: 0 },
            receiver: Some(receiver),
        };
        tokio::spawn(async move { <Game as Actor>::Envelope::run(actor).await });

        Self { sender }
    }
}

struct Player<G> {
    is_home: bool,
    game: G,
    receiver: Option<Receiver<PlayerEnvelop>>,
}
impl<G: ActorRef<Game>> Actor for Player<G> {
    type Envelope = PlayerEnvelop;

    fn receiver(&mut self) -> Receiver<Self::Envelope> {
        self.receiver.take().unwrap()
    }
}

actor_envelope! {
    PlayerEnvelop,
    Shoot => ShootMessage,
}

#[derive(Debug)]
struct ShootMessage;
impl Message for ShootMessage {
    type Result = ();
}

#[async_trait]
impl<G> Handler<ShootMessage> for Player<G>
where
    G: ActorRef<Game>,
{
    #[instrument(skip(self), ret(level = Level::DEBUG))]
    async fn handle(&mut self, _message: ShootMessage) -> <ScoreMessage as Message>::Result {
        // Player always scores two points
        let message = if self.is_home {
            ScoreMessage {
                scores: Scores { home: 2, away: 0 },
            }
        } else {
            ScoreMessage {
                scores: Scores { home: 0, away: 2 },
            }
        };
        self.game.notify(message).await.unwrap();
    }
}

struct PlayerRef<G> {
    sender: Sender<PlayerEnvelop>,
    marker: PhantomData<G>,
}
impl<G: ActorRef<Game>> PlayerRef<G> {
    pub fn new(is_home: bool, game: G) -> Self {
        let (sender, receiver) = ceramic_actor::channel(8);
        let actor = Player {
            is_home,
            game,
            receiver: Some(receiver),
        };
        tokio::spawn(async move { <Player<G> as Actor>::Envelope::run(actor).await });

        Self {
            sender,
            marker: Default::default(),
        }
    }
}

#[async_trait]
impl<G: ActorRef<Game>> ActorRef<Player<G>> for PlayerRef<G> {
    fn sender(&self) -> Sender<<Player<G> as Actor>::Envelope> {
        self.sender.clone()
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .pretty()
        .init();
    let game = GameRef::new();
    let player_home = PlayerRef::new(true, game.clone());
    let player_away = PlayerRef::new(false, game.clone());
    player_home.notify(ShootMessage).await.unwrap();
    player_away.send(ShootMessage).await.unwrap();
    player_home.send(ShootMessage).await.unwrap();
    println!(
        "Game score is: {:?}",
        game.send(GetScoreMessage).await.unwrap()
    );
}
