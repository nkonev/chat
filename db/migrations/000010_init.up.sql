CREATE OR REPLACE FUNCTION strip_tags(TEXT) RETURNS TEXT AS $$
SELECT regexp_replace($1, '<[^>]*>', '', 'g')
$$ LANGUAGE SQL;

create sequence chat_id_sequence;

create table chat_common(
    id bigint primary key,
    title varchar(512) not null,
    last_generated_message_id bigint not null default 0,
    create_date_time timestamp not null,
    blog boolean not null default false,
    can_resend boolean not null default false
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

create table chat_user_view(
    id bigint not null,
    title varchar(512) not null,
    pinned boolean not null default false,
    user_id bigint not null,
    update_date_time timestamp not null,
    last_message_id bigint,
    last_message_content text,
    last_message_owner_id bigint,
    participants_count bigint,
    participant_ids bigint[],
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
SELECT create_distributed_table('unread_messages_user_view', 'user_id');

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
