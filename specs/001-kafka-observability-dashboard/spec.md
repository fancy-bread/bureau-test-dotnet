# Feature Specification: Kafka Observability Dashboard — Core Scaffold

**Feature Branch**: `001-kafka-observability-dashboard`
**Created**: 2026-05-02
**Status**: Draft
**Input**: User description: "scaffold core for a .NET observability dashboard for Kafka events in the bureau.runs topic"

## User Scenarios & Testing

### User Story 1 — Live Event Feed (Priority: P1)

An operator opens the dashboard and sees a live feed of events as they arrive on the `bureau.runs` Kafka topic. Events appear in near-real-time without manual refresh.

**Why this priority**: This is the core value of an observability dashboard — seeing what is happening now. Everything else builds on this.

**Independent Test**: Publish a test event to `bureau.runs` and verify it appears on the dashboard feed within an observable time window. The dashboard delivers value even without any other feature.

**Acceptance Scenarios**:

1. **Given** the dashboard is running and connected to a Kafka cluster, **When** an event is published to the `bureau.runs` topic, **Then** the event appears in the dashboard feed within 2 seconds.
2. **Given** the dashboard is running, **When** no events have been published, **Then** the dashboard displays an empty feed with a visible connection status indicator.
3. **Given** the dashboard is running, **When** the Kafka connection is lost, **Then** the dashboard displays a disconnected status within 5 seconds.

---

### User Story 2 — Event Detail Inspection (Priority: P2)

An operator selects an event from the feed and views its full details: timestamp, event type, and complete payload.

**Why this priority**: The feed gives awareness; detail inspection gives understanding. Together they form the minimum viable observability workflow.

**Independent Test**: With at least one event in the feed, select it and verify that timestamp, event type, and full payload are displayed. Can be validated without Story 1's real-time behaviour.

**Acceptance Scenarios**:

1. **Given** at least one event is visible in the feed, **When** the operator selects it, **Then** the event's timestamp, type, and full payload are displayed.
2. **Given** an event is selected, **When** a new event arrives, **Then** the detail view is not disrupted.

---

### Edge Cases

- What happens when an event payload is malformed or cannot be parsed?
- How does the dashboard behave if the `bureau.runs` topic has no messages at startup?
- What is shown when the event history exceeds the display limit?

## Requirements

### Functional Requirements

- **FR-001**: The system MUST consume events from the `bureau.runs` Kafka topic continuously while the dashboard is running.
- **FR-002**: The system MUST display incoming events in a scrollable feed, ordered by arrival time, newest first.
- **FR-003**: Each event in the feed MUST show at minimum: timestamp, event type, and a payload summary.
- **FR-004**: The system MUST allow the Kafka broker endpoint and consumer group to be supplied via configuration, not hardcoded.
- **FR-005**: The system MUST display the current Kafka broker connection status at all times.
- **FR-006**: The system MUST retain and display at least the 100 most recent events without performance degradation.
- **FR-007**: Selecting an event from the feed MUST display its complete payload.
- **FR-008**: The system MUST handle malformed or unparseable event payloads gracefully, showing an error indicator rather than crashing or stopping event consumption.

### Key Entities

- **Bureau Event**: A message received from the `bureau.runs` topic. Key attributes: offset, partition, timestamp, event type, raw payload.
- **Kafka Connection**: The configured connection to a Kafka cluster. Key attributes: broker endpoint, topic (`bureau.runs`), consumer group ID, connection status (connected / disconnected / error).

## Success Criteria

### Measurable Outcomes

- **SC-001**: Events appear in the dashboard feed within 2 seconds of being published to `bureau.runs`.
- **SC-002**: The dashboard displays the last 100 events without visible performance degradation.
- **SC-003**: An operator can identify the event type and timestamp of any event in the feed within 5 seconds of it arriving.
- **SC-004**: Connection status updates within 5 seconds of a broker connectivity change.
- **SC-005**: A malformed event payload does not cause the dashboard to crash or stop consuming events.

## Assumptions

- The `bureau.runs` Kafka topic exists and is accessible from the environment where the dashboard runs.
- Events on the `bureau.runs` topic are formatted as [CloudEvents 1.0](https://cloudevents.io/) produced by the bureau CLI. The CloudEvents `type` attribute (e.g., `com.fancybread.bureau.run.started`) identifies the event. The `source` attribute follows the pattern `urn:bureau:run:{run_id}` or `urn:bureau:instance:{instance_id}:run:{run_id}`. The `data` field is a JSON object whose shape varies by event type.
- No Kafka authentication is required for the initial scaffold; authentication configuration may be added in a later spec.
- The dashboard is a browser-accessible web interface; native desktop or terminal UI is out of scope for this spec.
- A single consumer group is sufficient; multi-consumer or competing consumer scenarios are out of scope.
- Event history is held in memory; persistent storage across restarts is out of scope for this scaffold.
