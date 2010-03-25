drop schema if exists followship cascade;
create schema followship;
set search_path to followship, public;

CREATE SEQUENCE followships_seq;

create table followships
(
    id int primary key default nextval('followships_seq'),
    follower_id int,
    friend_id int
);

create index followships_follower_friend_idx on followships(follower_id, friend_id);
create index followships_friend_follower_idx on followships(friend_id, follower_id);

CREATE SEQUENCE followship_rollups_seq;

create or replace function my_array_length(ary anyarray)
returns integer
LANGUAGE SQL AS $$
    select coalesce(array_length($1, 1), 0);
$$;

create table followship_rollups
(
    max_id int not null, -- for sorting
    user_id int not null,
    append_frozen bool default false not null,
    follower_ids int[] not null CHECK (my_array_length(follower_ids) <= 100),
    friend_ids int[] not null CHECK (my_array_length(friend_ids) <= 100)
);

create unique index only_one_non_frozen on followship_rollups(user_id, NULLIF(append_frozen, true)); -- NULLS are always unique from each other

create index followship_rollups_user_idx on followship_rollups(user_id);
create index followship_rollups_append_frozen_idx on followship_rollups(append_frozen);

create index followship_rollups_array_length_followers_idx on followship_rollups(array_length(follower_ids, 1));
create index followship_rollups_array_length_friends_idx on followship_rollups(array_length(friend_ids, 1));

create index followship_rollups_expanded_follower on followship_rollups using gin (follower_ids  gin__int_ops);
create index followship_rollups_expanded_friend on followship_rollups using gin (friend_ids  gin__int_ops);


-- This takes twice as long as array_agg, but we can't use the nils
create or replace function array_agg_transfn_override (state anyarray, new anyelement) 
returns anyarray as $$
begin
    if $2 is null then return $1;
    else return  array_append($1, $2);
    end if;
end
$$ LANGUAGE plpgsql;
-- :(
CREATE AGGREGATE array_accum (anyelement) (
    sfunc = array_agg_transfn_override,
    stype = anyarray,
    initcond = '{}'
);

CREATE OR REPLACE FUNCTION move_friends() RETURNS void AS $$
DECLARE
start timestamptz;
BEGIN
    --TODO have this delete the stuff from followships
    raise notice 'copying to staging'; start := timeofday()::timestamptz;
    create temporary table followship_staging as select * from followships for update;
    raise notice 'staging copied  took %', timeofday()::timestamptz - start;

    create temporary view followship_batches_union as
    (select  l.friend_id as user_id,
            trunc((
                (count(*) OVER (partition by l.friend_id order by l.id)) +
                case when fs.follower_ids is null then 100 else my_array_length(fs.follower_ids) end - 1)
                / 100.0) as batch,
            l.id as follower_ord_id,
            l.follower_id as follower_id,
            null::integer as friend_ord_id,
            null::integer as friend_id
            from followship_staging as l
            left outer join followship_rollups as fs on (
                l.friend_id = fs.user_id and fs.append_frozen is false))
    union all
    (select  r.follower_id as user_id,
            trunc((
                (count(*) OVER (partition by r.follower_id order by r.id)) +
                case when fs.friend_ids is null then 100 else my_array_length(fs.friend_ids) end - 1)
                / 100.0) as batch,
            null::integer as follower_ord_id,
            null::integer as follower_id,
            r.id as friend_ord_id,
            r.friend_id as friend_id
        from followship_staging as r
        left outer join followship_rollups as fs on (
            r.follower_id = fs.user_id and fs.append_frozen is false));

    raise notice 'rolling up data into temp table'; start := timeofday()::timestamptz;
    create temporary table followship_batches_rollup as
    select
        case when max(follower_ord_id) is not null then max(follower_ord_id) else max(friend_ord_id) end as id,
        user_id,
        batch,
        coalesce(array_accum(follower_id order by follower_ord_id desc), ARRAY[]::int[]) as follower_ids,
        coalesce(array_accum(friend_id order by friend_ord_id desc), ARRAY[]::int[]) as friend_ids
    from followship_batches_union
    group by user_id, batch;
    raise notice 'rollup finished. took %', timeofday()::timestamptz - start;


    -- Populate followers_a
    raise notice 'updating rollups'; start := timeofday()::timestamptz;
    update followship_rollups as fs
        set friend_ids   =  fbs.friend_ids   || coalesce(fs.friend_ids, ARRAY[]::int[]),
            follower_ids =  fbs.follower_ids || coalesce(fs.follower_ids, ARRAY[]::int[]),
            max_id = fbs.id,
            append_frozen = my_array_length(fbs.friend_ids) + my_array_length(fs.friend_ids) >= 100 or
                            my_array_length(fbs.follower_ids) + my_array_length(fs.follower_ids) >= 100
        from followship_batches_rollup as fbs
        where fbs.user_id = fs.user_id
              and fbs.batch = 0
              and fs.append_frozen is false;
    raise notice 'updating rollups finished. took %', timeofday()::timestamptz - start;

    raise notice 'inserting rollups'; start := timeofday()::timestamptz;
    insert into followship_rollups 
        select id, user_id, 
        my_array_length(follower_ids) >= 100 or my_array_length(friend_ids) >= 100 as append_frozen, 
        follower_ids, friend_ids
        from followship_batches_rollup
        where batch <> 0
        order by batch;
    raise notice 'inserting rollups finished. took %', timeofday()::timestamptz - start;

    raise notice 'deleting old rows'; start := timeofday()::timestamptz;
    delete from followships using followship_staging
        where followships.id = followship_staging.id;
    raise notice 'deleting finished. took %', timeofday()::timestamptz - start;

    drop view followship_batches_union;
    drop table followship_staging cascade;
    drop table followship_batches_rollup cascade;
END;
$$ LANGUAGE plpgsql;

create view friends_of_order_desc as 
    (select follower_id as user_id, friend_id from followships order by id desc)
    union all
    (select user_id, unnest(friend_ids) as friend_id from followship_rollups order by max_id desc)
    ;

create or replace function last_n_friends(user_id integer, n integer)
RETURNS TABLE (friend_ids integer)
LANGUAGE SQL AS $$
    select friend_id from friends_of_order_desc where user_id = $1 limit $2
$$;
