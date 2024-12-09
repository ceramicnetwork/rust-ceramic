use std::ops::AddAssign;

use async_trait::async_trait;
use ceramic_actor::{actor_envelope, Actor, ActorHandle, Handler, Message};
use tracing::{instrument, Level};

#[derive(Actor)]
struct Game {
    scores: Scores,
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
#[actor(envelope = "PlayerEnv", handle = "PlayerH")]
struct Player<G: ActorHandle<Game>> {
    is_home: bool,
    game: G,
}

actor_envelope! {
    PlayerEnv,
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
    G: ActorHandle<Game>,
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .pretty()
        .init();
    let game = Game::spawn(1_000, Scores::default());
    let player_home = Player::spawn(1_000, true, game.clone());
    let player_away = Player::spawn(1_000, false, game.clone());
    player_home.notify(ShootMessage).await.unwrap();
    player_away.send(ShootMessage).await.unwrap();
    player_home.send(ShootMessage).await.unwrap();
    println!(
        "Game score is: {:?}",
        game.send(GetScoreMessage).await.unwrap()
    );
}
