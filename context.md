# Project Context: Betting Agent Backend

## Overview
This backend project is designed to ingest, process, and analyze sports betting data, focusing on player prop snapshots and hit rate calculations for NBA and MLB. It uses a modular repository and service structure, with PostgreSQL as the primary database.

## Key Concepts
- **Prop Snapshots**: Records of betting props (e.g., player points, rebounds) at specific times, ingested from external sources.
- **Player Snapshots**: Database records of player metadata (name, id, team, etc.).
- **Hit Rate Calculation**: Analytics to determine how often a player hits a given prop line over a recent set of games.

## Data Limitations
- **Prop Data**: Only includes player name (no team, event, or unique player id). This can cause ambiguity if multiple players share the same name.
- **Player Matching**: Player lookup is performed by first and last name only. If duplicates exist, the first match is used, and a warning is logged.

## Main Components
- **analytics/**: Hit rate calculators and utility functions.
- **repositories/**: Data access layers for props, players, games, and teams.
- **services/**: Ingestion and pipeline orchestration.
- **db/**: Database connection and interface logic.
- **migrations/**: SQL migration scripts for schema setup.
- **tests/**: Unit and integration tests.

## Known Issues & Warnings
- Ambiguous player names may lead to incorrect hit rate calculations as we have no current way to differentiate a player with the same name from team currently.
- Prop data should ideally be enriched with unique player identifiers for robust matching.

## Recommendations
- Handle multiple player matches by logging and skipping ambiguous cases, or by requesting more data from the provider.
- Document this limitation in analytics and ingestion code.

---
This file is intended to provide Copilot and developers with essential context for understanding the backend's structure, data flow, and limitations.
