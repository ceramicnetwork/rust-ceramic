use std::ops::AddAssign;

use async_trait::async_trait;
use ceramic_actor::{actor_envelope, Actor, ActorHandle, Error, Handler, Message};
use shutdown::Shutdown;
use tracing::{instrument, Level};

#[derive(Actor)]
pub struct Game {
    scores: Scores,
}
impl Game {
    pub fn new() -> Self {
        Self {
            scores: Default::default(),
        }
    }
}

actor_envelope! {
    GameEnvelope,
    GameActor,
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

#[derive(Clone, Debug, Default)]
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

#[derive(Actor)]
// The envelope and handle types names can be explicitly named.
#[actor(envelope = "PlayerEnv", handle = "PlayerH", actor_trait = "PlayerI")]
pub struct Player {
    is_home: bool,
    game: GameHandle,
}

impl Player {
    fn new(is_home: bool, game: GameHandle) -> Self {
        Self { is_home, game }
    }
}

actor_envelope! {
    PlayerEnv,
    PlayerI,
    Shoot => ShootMessage,
}

#[derive(Debug)]
struct ShootMessage;
impl Message for ShootMessage {
    type Result = ();
}

#[async_trait]
impl Handler<ShootMessage> for Player {
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .pretty()
        .init();
    let shutdown = Shutdown::new();
    let (game, _) = Game::spawn(1_000, Game::new(), shutdown.wait_fut());
    let (player_home, _) =
        Player::spawn(1_000, Player::new(true, game.clone()), shutdown.wait_fut());
    let (player_away, _) =
        Player::spawn(1_000, Player::new(false, game.clone()), shutdown.wait_fut());
    player_home.notify(ShootMessage).await.unwrap();
    player_away.send(ShootMessage).await.unwrap();
    // Send with retry without cloning the message to be sent.
    let mut msg = ShootMessage;
    loop {
        match player_home.send(msg).await {
            Ok(_) => break,
            Err(Error::Send { message }) => msg = message.0,
            Err(_) => panic!(),
        };
    }
    println!(
        "Game score is: {:?}",
        game.send(GetScoreMessage).await.unwrap()
    );
    shutdown.shutdown();
}