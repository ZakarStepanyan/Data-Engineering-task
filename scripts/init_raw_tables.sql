DROP DATABASE IF EXISTS crm_raw;
CREATE DATABASE crm_raw;

USE crm_raw;

CREATE TABLE IF NOT EXISTS crm_raw.activity_types (
    id String,
    name String,
    active String,
    type String
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS crm_raw.activity (
    activity_id String,
    type String,
    assigned_to_user String,
    deal_id String,
    done String,
    due_to String
) ENGINE = MergeTree() ORDER BY activity_id;

CREATE TABLE IF NOT EXISTS crm_raw.deal_changes (
    deal_id String,
    change_time String,
    changed_field_key String,
    new_value String
) ENGINE = MergeTree() ORDER BY change_time;

CREATE TABLE IF NOT EXISTS crm_raw.fields (
    id String,
    field_key String,
    name String,
    field_value_options String
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS crm_raw.stages (
    stage_id String,
    stage_name String
) ENGINE = MergeTree() ORDER BY stage_id;

CREATE TABLE IF NOT EXISTS crm_raw.users (
    id String,
    name String,
    email String,
    modified String
) ENGINE = MergeTree() ORDER BY id;