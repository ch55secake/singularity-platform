use std::collections::HashMap;

use serde::Serialize;

pub(super) const IMPACT_SCORE_VERSION: u32 = 1;

#[derive(Debug, Serialize, Clone, Copy, Default)]
pub(super) struct ImpactScore {
    pub(super) score: f64,
    pub(super) combat: f64,
    pub(super) economy: f64,
    pub(super) utility: f64,
    pub(super) survival: f64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ImpactMetrics {
    pub(super) slot: u32,
    pub(super) hero_damage: f64,
    pub(super) kills: f64,
    pub(super) deaths: f64,
    pub(super) assists: f64,
    pub(super) souls: f64,
    pub(super) last_hits: f64,
    pub(super) denies: f64,
    pub(super) healing: f64,
    pub(super) damage_mitigated: f64,
}

pub(super) fn calculate_impact_scores(
    duration_s: u32,
    players: &[ImpactMetrics],
) -> HashMap<u32, ImpactScore> {
    if players.is_empty() {
        return HashMap::new();
    }

    let minutes = f64::from(duration_s.max(60)) / 60.0;
    let rates = players
        .iter()
        .map(|player| {
            [
                player.hero_damage / minutes,
                (player.kills + player.assists) / minutes,
                player.souls / minutes,
                (player.last_hits + player.denies) / minutes,
                player.healing / minutes + player.damage_mitigated / minutes,
                player.deaths / minutes,
            ]
        })
        .collect::<Vec<_>>();
    let z_scores = (0..6)
        .map(|metric| standard_scores(&rates.iter().map(|row| row[metric]).collect::<Vec<_>>()))
        .collect::<Vec<_>>();

    players
        .iter()
        .enumerate()
        .map(|(index, player)| {
            let combat = component_score((z_scores[0][index] + z_scores[1][index]) / 2.0);
            let economy = component_score((z_scores[2][index] + z_scores[3][index]) / 2.0);
            let utility = component_score(z_scores[4][index]);
            let survival = component_score(-z_scores[5][index]);
            let score =
                round_score(combat * 0.40 + economy * 0.25 + utility * 0.20 + survival * 0.15);
            (
                player.slot,
                ImpactScore {
                    score,
                    combat,
                    economy,
                    utility,
                    survival,
                },
            )
        })
        .collect()
}

fn standard_scores(values: &[f64]) -> Vec<f64> {
    if values.len() < 2 {
        return vec![0.0; values.len()];
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let deviation = (values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64)
        .sqrt();
    if deviation <= f64::EPSILON {
        return vec![0.0; values.len()];
    }
    values
        .iter()
        .map(|value| ((value - mean) / deviation).clamp(-2.5, 2.5))
        .collect()
}

fn component_score(value: f64) -> f64 {
    round_score(value.clamp(-2.5, 2.5) * 2.0)
}

pub(super) fn round_score(value: f64) -> f64 {
    (value.clamp(-5.0, 5.0) * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn impact_scores_are_neutral_for_identical_lobby_stats() {
        let baseline = ImpactMetrics {
            slot: 1,
            hero_damage: 1000.0,
            kills: 2.0,
            deaths: 2.0,
            assists: 4.0,
            souls: 12000.0,
            last_hits: 80.0,
            denies: 4.0,
            healing: 100.0,
            damage_mitigated: 200.0,
        };
        let players = [
            baseline,
            ImpactMetrics {
                slot: 2,
                ..baseline
            },
        ];

        let scores = calculate_impact_scores(600, &players);

        assert_eq!(scores[&1].score, 0.0);
        assert_eq!(scores[&2].score, 0.0);
    }

    #[test]
    fn impact_score_rewards_broad_contribution_and_stays_bounded() {
        let baseline = ImpactMetrics {
            slot: 1,
            hero_damage: 1000.0,
            kills: 2.0,
            deaths: 2.0,
            assists: 4.0,
            souls: 12000.0,
            last_hits: 80.0,
            denies: 4.0,
            healing: 100.0,
            damage_mitigated: 200.0,
        };
        let standout = ImpactMetrics {
            slot: 3,
            hero_damage: 6000.0,
            kills: 9.0,
            deaths: 1.0,
            assists: 12.0,
            souls: 25000.0,
            last_hits: 170.0,
            denies: 12.0,
            healing: 1600.0,
            damage_mitigated: 1800.0,
        };

        let scores = calculate_impact_scores(
            1200,
            &[
                baseline,
                ImpactMetrics {
                    slot: 2,
                    ..baseline
                },
                standout,
            ],
        );
        let strong = scores[&3];

        assert!(strong.score > 0.0);
        assert!(scores[&1].score < 0.0);
        for score in scores.values() {
            assert!((-5.0..=5.0).contains(&score.score));
            assert!((-5.0..=5.0).contains(&score.combat));
            assert!((-5.0..=5.0).contains(&score.economy));
            assert!((-5.0..=5.0).contains(&score.utility));
            assert!((-5.0..=5.0).contains(&score.survival));
        }
    }
}
