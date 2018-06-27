drop table if exists queries;

create table queries (
  query_id text primary key,
  query_text text not null,
  status text not null,
  bucket text,
  key text
);
