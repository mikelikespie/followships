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

create table followship_rollups
(
    max_id int not null, -- for sorting
    user_id int not null,
    append_frozen bool default false not null,
    follower_ids int[],
    friend_ids int[]
);

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
BEGIN
    create temporary table followship_staging as select * from followships;

    create temporary view followship_batches_l as
    select  l.friend_id as user_id,
            trunc((
                (rank() OVER (partition by l.friend_id order by l.id)) +
                    COALESCE(array_length(fs.follower_ids,1), 100) - 1)
                / 100.0) as batch,
            l.id as follower_ord_id,
            l.follower_id as follower_id,
            null::integer as friend_ord_id,
            null::integer as friend_id
            from followship_staging as l
            left outer join followship_rollups as fs on (
                l.friend_id = fs.user_id and fs.append_frozen is false);

    create temporary view followship_batches_r as
    select  r.follower_id as user_id,
            trunc((
                (rank() OVER (partition by r.follower_id order by r.id)) +
                    COALESCE(array_length(fs.friend_ids,1), 100) - 1)
                / 100.0) as batch,
            null::integer as follower_ord_id,
            null::integer as follower_id,
            r.id as friend_ord_id,
            r.friend_id as friend_id
        from followship_staging as r
        left outer join followship_rollups as fs on (
            r.follower_id = fs.user_id and fs.append_frozen is false);

    create temporary view followship_batches_union as
    (select * from followship_batches_l)
    union all
    (select * from followship_batches_r);

    create temporary table followship_batches_rollup as
    select
        case when max(follower_ord_id) is not null then max(follower_ord_id) else max(friend_ord_id) end as id,
        user_id,
        batch,
        (array_accum(follower_id order by follower_ord_id desc) ) as follower_ids,
        (array_accum(friend_id order by friend_ord_id desc)) as friend_ids
    from followship_batches_union
    group by user_id, batch;

    -- Populate followers_a
    update followship_rollups as fs
        set friend_ids   =  fbs.friend_ids   || fs.friend_ids,
            follower_ids =  fbs.follower_ids || fs.follower_ids,
            max_id = fbs.id,
            append_frozen = coalesce(array_length(fbs.friend_ids, 1) + array_length(fs.friend_ids, 1) >= 100, false) or
                            coalesce(array_length(fbs.follower_ids, 1) + array_length(fs.follower_ids, 1) >= 100, false)
        from followship_batches_rollup as fbs
        where fbs.user_id = fs.user_id
              and fbs.batch = 0
              and fs.append_frozen is false;

    insert into followship_rollups 
        select id, user_id, 
        coalesce(array_length(follower_ids, 1), 0) >= 100 or coalesce(array_length(friend_ids, 1), 0) >= 100 as append_frozen, 
        follower_ids, friend_ids
        from followship_batches_rollup
        order by batch;

    drop table followship_staging cascade;
    drop table followship_batches_rollup cascade;
END;
$$ LANGUAGE plpgsql;
