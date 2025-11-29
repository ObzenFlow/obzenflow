use super::domain::{PaymentCommand, TrafficPhase};

/// Scripted traffic pattern used by the demo source.
///
/// The sequence is deliberately small and predictable so it is
/// easy to reason about circuit breaker behaviour:
///
/// - First 8 events (Warmup): all locally valid, gateway healthy.
/// - Next 10 events (Outage): locally valid, gateway starts failing.
/// - Final 7 events (Recovery): mix of valid/invalid while gateway
///   is healthy again.
pub fn scripted_commands() -> Vec<PaymentCommand> {
    let mut commands = Vec::new();

    // Warmup: healthy dependency, all good cards.
    for i in 0..8 {
        commands.push(PaymentCommand {
            request_id: format!("warmup-{}", i),
            customer_id: format!("cust-{}", i),
            amount_cents: 10_00,
            card_ok: true,
            phase: TrafficPhase::Warmup,
        });
    }

    // Outage: dependency begins to fail even for good cards.
    for i in 0..10 {
        commands.push(PaymentCommand {
            request_id: format!("outage-{}", i),
            customer_id: format!("cust-{}", 100 + i),
            amount_cents: 20_00,
            card_ok: true,
            phase: TrafficPhase::Outage,
        });
    }

    // Recovery: dependency is healthy again, but we also inject a
    // couple of locally invalid commands to exercise validation
    // error metrics separately from circuit breaker behaviour.
    let recovery_pattern = [
        (true, 30_00),
        (false, 30_00), // bad card
        (true, 0),      // zero amount
        (true, 15_00),
        (false, 15_00), // bad card
        (true, 50_00),
        (true, 50_00),
    ];

    for (idx, (card_ok, amount_cents)) in recovery_pattern.into_iter().enumerate() {
        commands.push(PaymentCommand {
            request_id: format!("recovery-{}", idx),
            customer_id: format!("cust-{}", 200 + idx),
            amount_cents,
            card_ok,
            phase: TrafficPhase::Recovery,
        });
    }

    commands
}
