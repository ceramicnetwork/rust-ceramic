use std::{marker::PhantomData, ops::AddAssign, time::Duration};

use async_trait::async_trait;
use ceramic_actor::{actor_message, Actor, ActorRef, Handler, Message, TracedMessage};
use tokio::sync::mpsc;
use tracing::{instrument, Instrument, Level};

struct Game {
    scores: Scores,
    receiver: Option<mpsc::Receiver<TracedMessage<GameMessage>>>,
}

impl Actor for Game {
    type Envelope = GameMessage;

    fn receiver(&mut self) -> mpsc::Receiver<TracedMessage<Self::Envelope>> {
        self.receiver.take().unwrap()
    }
}

actor_message! {
    GameMessage,
    notify Score => ScoreMessage,
    send GetScore => GetScoreMessage,
}

impl Game {
    async fn run(self) {
        GameMessage::run(self).await
    }
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
    sender: mpsc::Sender<TracedMessage<GameMessage>>,
}

impl ActorRef<Game> for GameRef {
    fn sender(&self) -> mpsc::Sender<TracedMessage<<Game as Actor>::Envelope>> {
        self.sender.clone()
    }
}

impl GameRef {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor = Game {
            scores: Scores { home: 0, away: 0 },
            receiver: Some(receiver),
        };
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }
}

struct Player<G> {
    is_home: bool,
    game: G,
    receiver: Option<mpsc::Receiver<TracedMessage<PlayerMessage>>>,
}
impl<G: ActorRef<Game>> Actor for Player<G> {
    type Envelope = PlayerMessage;

    fn receiver(&mut self) -> mpsc::Receiver<TracedMessage<Self::Envelope>> {
        self.receiver.take().unwrap()
    }
}
impl<G: ActorRef<Game>> Player<G> {
    async fn run(self) {
        PlayerMessage::run(self).await
    }
}

actor_message! {
    PlayerMessage,
    notify Shoot => ShootMessage,
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
    sender: mpsc::Sender<TracedMessage<PlayerMessage>>,
    marker: PhantomData<G>,
}
impl<G: ActorRef<Game>> PlayerRef<G> {
    pub fn new(is_home: bool, game: G) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let actor = Player {
            is_home,
            game,
            receiver: Some(receiver),
        };
        tokio::spawn(async move { actor.run().await });

        Self {
            sender,
            marker: Default::default(),
        }
    }
}

impl<G: ActorRef<Game>> ActorRef<Player<G>> for PlayerRef<G> {
    fn sender(&self) -> mpsc::Sender<TracedMessage<<Player<G> as Actor>::Envelope>> {
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
    player_away.notify(ShootMessage).await.unwrap();
    player_home.notify(ShootMessage).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    println!(
        "Game score is: {:?}",
        game.send(GetScoreMessage).await.unwrap()
    );
}
