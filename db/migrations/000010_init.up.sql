CREATE OR REPLACE FUNCTION strip_tags(TEXT) RETURNS TEXT AS $$
SELECT regexp_replace($1, '<[^>]*>', '', 'g')
$$ LANGUAGE SQL;

create sequence chat_id_sequence;

create table chat_common(
    id bigint primary key,
    title varchar(512) not null,
    last_generated_message_id bigint not null default 0,
    create_date_time timestamp not null,
    can_resend boolean not null default false,
    tet_a_tet boolean not null default false,
    available_to_search boolean not null default false,
    avatar text,
    avatar_big text,
    regular_participant_can_write_message BOOLEAN NOT NULL DEFAULT TRUE
);

create table chat_participant(
    user_id bigint not null,
    chat_id bigint not null,
    create_date_time timestamp not null,
    chat_admin boolean not null default false,
    primary key(user_id, chat_id)
);
SELECT create_distributed_table('chat_participant', 'chat_id');

create table message(
    id bigint not null,
    chat_id bigint not null,
    owner_id bigint not null,
    content text not null,
    blog_post boolean not null default false,
    embed_message_id bigint,
    embed_chat_id bigint,
    embed_owner_id bigint,
    embed_message_type varchar(16),
    create_date_time timestamp not null,
    update_date_time timestamp,
    primary key (chat_id, id)
);
SELECT create_distributed_table('message', 'chat_id');

CREATE TABLE message_reaction(
    chat_id BIGINT,
    user_id BIGINT NOT NULL,
    reaction VARCHAR(4) NOT NULL,
    message_id BIGINT NOT NULL,
    create_date_time timestamp not null,
    PRIMARY KEY (chat_id, message_id, user_id, reaction),
    FOREIGN KEY (message_id, chat_id) REFERENCES message(id, chat_id) ON DELETE CASCADE
);

-- https://docs.citusdata.com/en/v11.1/develop/api_udf.html#example
SELECT create_distributed_table('message_reaction', 'chat_id', colocate_with => 'message');

create table chat_user_view(
    id bigint not null,
    pinned boolean not null default false,
    user_id bigint not null,
    update_date_time timestamp not null,
    last_message_id bigint,
    last_message_content text,
    last_message_owner_id bigint,
    participants_count bigint,
    participant_ids bigint[],
    consider_messages_as_unread BOOLEAN not null default true,
    unread_messages bigint not null default 0,
    last_read_message_id bigint not null default 0,
    primary key (user_id, id)
);
SELECT create_distributed_table('chat_user_view', 'user_id');

create table unread_messages_user_view(
    user_id bigint not null,
    chat_id bigint not null,
    unread_messages bigint not null default 0,
    last_message_id bigint not null default 0,
    primary key (user_id, chat_id)
);
SELECT create_distributed_table('unread_messages_user_view', 'user_id', colocate_with => 'chat_user_view');

create table has_unread_messages(user_id bigint primary key, has boolean not null default false);
SELECT create_distributed_table('has_unread_messages', 'user_id');

create table technical(
    id int primary key,
    need_to_fast_forward_sequences bool not null default false
);

create table blog(
    id int primary key,
    owner_id bigint,
    title varchar(256) not null,
    post text,
    preview varchar(512),
    create_date_time timestamp not null
);
