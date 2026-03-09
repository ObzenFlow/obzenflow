// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::*;

pub fn carriers() -> Vec<CarrierDetails> {
    vec![
        CarrierDetails {
            carrier_code: "AA".to_string(),
            carrier_name: "American Airlines".to_string(),
            country: "USA".to_string(),
            fleet_size: 950,
        },
        CarrierDetails {
            carrier_code: "UA".to_string(),
            carrier_name: "United Airlines".to_string(),
            country: "USA".to_string(),
            fleet_size: 850,
        },
        CarrierDetails {
            carrier_code: "DL".to_string(),
            carrier_name: "Delta Air Lines".to_string(),
            country: "USA".to_string(),
            fleet_size: 900,
        },
        CarrierDetails {
            carrier_code: "WN".to_string(),
            carrier_name: "Southwest Airlines".to_string(),
            country: "USA".to_string(),
            fleet_size: 750,
        },
        CarrierDetails {
            carrier_code: "BA".to_string(),
            carrier_name: "British Airways".to_string(),
            country: "UK".to_string(),
            fleet_size: 290,
        },
        CarrierDetails {
            carrier_code: "LH".to_string(),
            carrier_name: "Lufthansa".to_string(),
            country: "Germany".to_string(),
            fleet_size: 340,
        },
        CarrierDetails {
            carrier_code: "AF".to_string(),
            carrier_name: "Air France".to_string(),
            country: "France".to_string(),
            fleet_size: 280,
        },
    ]
}

pub fn flights() -> Vec<FlightRecord> {
    let raw = vec![
        ("AA", "2023-12-01", "LAX", "JFK", 120, 15),
        ("DL", "2023-12-01", "ATL", "ORD", 90, 0),
        ("UA", "2023-12-01", "SFO", "LAX", 60, 45),
        ("AA", "2023-12-01", "DFW", "MIA", 180, 5),
        ("WN", "2023-12-01", "LAS", "PHX", 75, 120),
        ("DL", "2023-12-01", "SEA", "DEN", 110, 8),
        ("UA", "2023-12-01", "EWR", "SFO", 300, 0),
        ("AA", "2023-12-01", "ORD", "LAX", 240, 25),
    ];

    raw.into_iter()
        .enumerate()
        .map(
            |(idx, (carrier, date, origin, destination, scheduled_duration, delay_minutes))| {
                let flight_number = format!("{}{}", carrier, 1000 + (idx + 1) * 100);
                FlightRecord {
                    carrier: carrier.to_string(),
                    date: date.to_string(),
                    origin: origin.to_string(),
                    destination: destination.to_string(),
                    scheduled_duration,
                    delay_minutes,
                    flight_number,
                    delay_category: None,
                }
            },
        )
        .collect()
}
