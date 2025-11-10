CREATE DATABASE moodle;


-- The 'USE' statement must be run in a separate context or session if the client requires it,
-- but often works fine immediately after CREATE DATABASE without a batch separator.
USE moodle;


-- =======================================
-- 1. DIMENSIONS
-- =======================================

-- (1) Actor: người thực hiện hành động
CREATE TABLE dim_actor_account (
    actor_account_name NVARCHAR(255) NOT NULL,        -- actor.account.name
    actor_account_homePage NVARCHAR(500) NOT NULL,    -- actor.account.homePage
    actor_name NVARCHAR(255),                         -- actor.name
    actor_objectType NVARCHAR(100),                   -- actor.objectType
    CONSTRAINT PK_dim_actor_account PRIMARY KEY (actor_account_name)
);


-- (2) Verb: hành động (Viewed, Completed, ...)
CREATE TABLE dim_verb (
    verb_id NVARCHAR(255) PRIMARY KEY,                -- verb.id
    verb_display NVARCHAR(255)                        -- verb.display.en
);


-- (3) Activity details — chứa object hoặc contextActivities
CREATE TABLE activity_detail (
    activity_id NVARCHAR(500) PRIMARY KEY,            -- object.id hoặc contextActivities.*.id
    definition_name NVARCHAR(255),                    -- definition.name.en
    definition_type NVARCHAR(255),                    -- definition.type
    objectType NVARCHAR(100)                          -- objectType (Activity)
);


-- =======================================
-- 2. CONTEXT DIMENSION
-- =======================================

-- (4) Context: môi trường hoạt động + event metadata
CREATE TABLE dim_context (
    context_id UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY,
    context_registration NVARCHAR(255),               -- context.registration
    context_language NVARCHAR(50),                    -- context.language
    context_platform NVARCHAR(100),                   -- context.platform
    
    -- Event metadata (từ context.extensions.info)
    moodle_version NVARCHAR(50),                      -- extensions["http://moodle.org"]
    plugin_repo NVARCHAR(255),                        -- extensions["https://github.com/xAPI-vle/moodle-logstore_xapi"]
    event_name NVARCHAR(255),                         -- extensions.event_name
    event_function NVARCHAR(255)                      -- extensions.event_function
);


-- (5) contextActivities (category, parent, other)
CREATE TABLE bridge_context_activity (
    context_id UNIQUEIDENTIFIER NOT NULL,
    activity_id NVARCHAR(500) NOT NULL,
    role NVARCHAR(50) NOT NULL CHECK (role IN ('category', 'parent', 'other')),
    CONSTRAINT PK_bridge_context_activity PRIMARY KEY (context_id, activity_id, role),
    CONSTRAINT FK_bridge_context_activity_context FOREIGN KEY (context_id)
        REFERENCES dim_context(context_id),
    CONSTRAINT FK_bridge_context_activity_activity FOREIGN KEY (activity_id)
        REFERENCES activity_detail(activity_id)
);


-- =======================================
-- 3. FACT STATEMENT
-- =======================================

CREATE TABLE fact_statement (
    statement_id NVARCHAR(255) PRIMARY KEY,           -- statement.id
    actor_account_name NVARCHAR(255) NOT NULL,        -- actor.account.name
    verb_id NVARCHAR(255) NOT NULL,                   -- verb.id
    object_activity_id NVARCHAR(500) NOT NULL,        -- object.id (activity chính)
    context_id UNIQUEIDENTIFIER NULL,                 -- FK đến dim_context
    timestamp DATETIME2 NOT NULL,                     -- timestamp
    stored DATETIME2,                                 -- stored
    load_time DATETIME2 NOT NULL DEFAULT GETDATE(),   -- Time when the fact was loaded into the DWH
    version NVARCHAR(50),                             -- version
    authority_account_name NVARCHAR(255),             -- authority.account.name
    authority_account_homePage NVARCHAR(500),         -- authority.account.homePage

    CONSTRAINT FK_fact_statement_actor FOREIGN KEY (actor_account_name)
        REFERENCES dim_actor_account(actor_account_name),
    CONSTRAINT FK_fact_statement_verb FOREIGN KEY (verb_id)
        REFERENCES dim_verb(verb_id),
    CONSTRAINT FK_fact_statement_object FOREIGN KEY (object_activity_id)
        REFERENCES activity_detail(activity_id),
    CONSTRAINT FK_fact_statement_context FOREIGN KEY (context_id)
        REFERENCES dim_context(context_id)
);