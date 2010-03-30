drop schema if exists followship cascade;
create schema followship;
set search_path to followship, public;

CREATE SEQUENCE followships_seq;

create table followships
(
    id bigint primary key default nextval('followships_seq'),
    follower_id int,
    friend_id int
);

create index followships_follower_friend_idx on followships(follower_id, friend_id);
create index followships_friend_follower_idx on followships(friend_id, follower_id);

CREATE SEQUENCE followship_rollups_seq;

create or replace function my_array_length(ary anyarray)
returns integer
LANGUAGE SQL IMMUTABLE AS $$
    select coalesce(array_length($1, 1), 0);
$$;

create table followship_rollups
(
    user_id int primary key not null,
    follower_ids int[] not null,
    friend_ids int[] not null
);

create index followship_rollups_array_length_followers_idx on followship_rollups(user_id, my_array_length(follower_ids));
create index followship_rollups_array_length_friends_idx on followship_rollups(user_id, my_array_length(friend_ids));

create index followship_rollups_expanded_follower on followship_rollups using gin (follower_ids);
create index followship_rollups_expanded_friend on followship_rollups using gin (friend_ids);


-- This takes twice as long as array_agg, but we can't use the nils
create or replace function array_agg_transfn_override (state anyarray, new anyelement) 
returns anyarray as $$
begin
    if $2 is null then return $1;
    else return  array_append($1, $2);
    end if;
end
$$ LANGUAGE plpgsql IMMUTABLE ;

CREATE OR REPLACE FUNCTION move_friends_from(tablename text)
RETURNS void as $$
DECLARE start timestamptz;
BEGIN
    execute 'create temporary view followship_staging_v as select * from ' || quote_ident($1); -- Just make a view to make it easy

    create temporary view followship_batches_union as
    (select  l.friend_id as user_id,
            l.id as follower_ord_id,
            l.follower_id as follower_id,
            null::integer as friend_ord_id,
            null::integer as friend_id
            from followship_staging_v as l)
    union all
    (select  r.follower_id as user_id,
            null::integer as follower_ord_id,
            null::integer as follower_id,
            r.id as friend_ord_id,
            r.friend_id as friend_id
        from followship_staging_v as r);

    raise log 'rolling up data into temp table'; start := timeofday()::timestamptz;
    create temporary view followship_batches_rollup as
    select
        user_id,
        coalesce(array_accum(follower_id order by follower_ord_id desc), ARRAY[]::int[]) as follower_ids,
        coalesce(array_accum(friend_id order by friend_ord_id desc), ARRAY[]::int[]) as friend_ids
    from followship_batches_union
    group by user_id;
    raise log 'rollup finished. took %', timeofday()::timestamptz - start;


    -- Populate followers_a
    raise log 'updating rollups'; start := timeofday()::timestamptz;
    update followship_rollups as fs
        set friend_ids   =  fbs.friend_ids   || coalesce(fs.friend_ids, ARRAY[]::int[]),
            follower_ids =  fbs.follower_ids || coalesce(fs.follower_ids, ARRAY[]::int[])
        from followship_batches_rollup as fbs
        where fbs.user_id = fs.user_id;
    raise log 'updating rollups finished. took %', timeofday()::timestamptz - start;

    raise log 'inserting rollups'; start := timeofday()::timestamptz;
    insert into followship_rollups 
        select fbr.user_id, fbr.follower_ids, fbr.friend_ids
        from followship_batches_rollup fbr
        where fbr.user_id not in (select distinct user_id from followship_rollups); -- Instead of a not in. probably performs better
    raise log 'inserting rollups finished. took %', timeofday()::timestamptz - start;

    drop view followship_batches_rollup cascade;
    drop view followship_batches_union cascade;
    drop view followship_staging_v cascade;
END;
$$ LANGUAGE plpgsql;
    
CREATE OR REPLACE FUNCTION move_friends() RETURNS void AS $$
DECLARE
start timestamptz;
BEGIN
    --TODO have this delete the stuff from followships
    raise log 'copying to staging'; start := timeofday()::timestamptz;
    create temporary table followship_staging as select * from followships for update;
    raise log 'staging copied  took %', timeofday()::timestamptz - start;
    
    perform move_friends_from('followship_staging');

    raise log 'deleting old rows'; start := timeofday()::timestamptz;
    delete from followships using followship_staging
        where followships.id = followship_staging.id;
    raise log 'deleting finished. took %', timeofday()::timestamptz - start;

    drop table followship_staging cascade;
END;
$$ LANGUAGE plpgsql;


-- ONLY USE THIS when there's no stuff being loaded into followships
CREATE OR REPLACE FUNCTION bulk_load_friends(filepath text) RETURNS void AS $$
DECLARE
start timestamptz;
BEGIN

    create temporary table followship_staging
    (
        id int not null default nextval('followships_seq'),
        follower_id int,
        friend_id int
    );

    --TODO have this delete the stuff from followships
    raise notice 'copying to staging'; start := timeofday()::timestamptz;
    execute 'copy followship_staging (follower_id, friend_id) from ' || quote_literal($1) || ' with csv';
    raise notice 'staging copied  took %', timeofday()::timestamptz - start;
    
    raise notice 'moving friends from staging'; start := timeofday()::timestamptz;
    perform move_friends_from('followship_staging');
    raise notice 'move took  took %', timeofday()::timestamptz - start;

    drop table followship_staging cascade;
END;
$$ LANGUAGE plpgsql;


create view followers_of_order_desc as 
    (select friend_id as user_id, follower_id from followships order by id desc)
    union all
    (select user_id, unnest(follower_ids) as friend_id from followship_rollups)
    ;

create view friends_of_order_desc as 
    (select follower_id as user_id, friend_id from followships order by id desc)
    union all
    (select user_id, unnest(friend_ids) as friend_id from followship_rollups)
    ;

create or replace function last_n_friends(user_id integer, n integer)
RETURNS TABLE (friend_ids integer)
LANGUAGE SQL AS $$
    select friend_id from friends_of_order_desc where user_id = $1 limit $2
$$;

create or replace function last_n_followers(user_id integer, n integer)
RETURNS TABLE (follower_ids integer)
LANGUAGE SQL AS $$
    select follower_id from followers_of_order_desc where user_id = $1 limit $2
$$;

-- Return true or false if the friendship exists
create or replace function has_follower(user_id integer, follower_id integer)
returns boolean
language sql as $$
    (select true from followships where friend_id = $1 and follower_id = $2)
    union all
    (select true from followship_rollups where user_id = $1 and follower_ids @> ARRAY[$2])
    union all
    (select false)
    limit 1
    ;
$$;


-- Return true or false if the followership exists
create or replace function has_friend(user_id integer, friend_id integer)
returns boolean
language sql as $$
    (select true from followships where follower_id = $1 and friend_id = $2)
    union all
    (select true from followship_rollups where user_id = $1 and friend_ids @> ARRAY[$2])
    union all
    (select false)
    limit 1
    ;
$$;

CREATE OR REPLACE FUNCTION delete_followship(follower integer, friend integer) RETURNS void AS $$
DECLARE
BEGIN
    delete from followships where follower_id = $1 and friend_id = $2;

    update followship_rollups
        set follower_ids = followship_intarray_del_elem(follower_ids, $1)
        where followship_rollups.user_id = $2 and follower_ids @> ARRAY[$1];

    update followship_rollups
        set friend_ids = followship_intarray_del_elem(friend_ids, $2)
        where followship_rollups.user_id = $1 and friend_ids @> ARRAY[$1];
        
END
$$ LANGUAGE plpgsql;

create or replace function num_followers(user_id integer)
returns int8
language sql as $$
    select sum(c)::int8 from ( 
        (select my_array_length(follower_ids) as c from followship_rollups where user_id = $1)
        union all
        (select count(*) as c from followships where friend_id = $1)
    ) as foo;
$$;

create or replace function num_friends(user_id integer)
returns int8
language sql as $$
    select sum(c)::int8 from ( 
        (select my_array_length(friend_ids) as c from followship_rollups where user_id = $1)
        union all
        (select count(*) as c from followships where follower_id = $1)
    ) as foo;
$$;

