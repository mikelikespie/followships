drop schema if exists social cascade;
create schema social;
set search_path to social, public;

create language plpgsql;

CREATE SEQUENCE friendships_seq;

create table friendships
(
    id int not null default nextval('friendships_seq'),
    follower int,
    friend int
);

create index friendships_follower_friend_idx on friendships(follower, friend);
create index friendships_friend_follower_idx on friendships(friend, follower);

CREATE SEQUENCE followers_a_seq;

create table followers_a
(
    id int not null default nextval('followers_a_seq'),
    followers int[],
    friend int,
    --created_at timestamptz default now(),
    append_frozen bool default false
);

create index followers_a_friend_idx on followers_a(friend);
create index followers_a_append_frozen_idx on followers_a(append_frozen);
create index followers_a_array_length_followers_idx on followers_a(array_length(followers, 1));
create index followers_a_expanded on followers_a using gin (followers  gin__int_ops);

-- Unpacsk and unions with the 
create view friendships_friend_to_follower as 
    select unnest(followers) as follower, friend from followers_a
    union all select follower, friend from friendships;

-- does same, but ordered
create view friendships_friend_to_follower_ordered_desc as 
    (select follower, friend from friendships order by id desc)
    union all 
    (select unnest(followers) as follower, friend from followers_a order by id desc)
    ;

CREATE SEQUENCE friends_a_seq;

create table friends_a
( 
    id int not null default nextval('friends_a_seq'),
    follower int,
    friends int[],
    -- created_at timestamptz default now(),
    append_frozen bool default false
);
create index friends_a_follower_idx on friends_a(follower);
create index friends_a_append_frozen_idx on friends_a(append_frozen);
create index friends_a_array_length_friends_idx on friends_a(array_length(friends, 1));
create index friends_a_expanded on friends_a using gin (friends  gin__int_ops);

--unpacs friends_a
create view friendships_follower_to_friend as 
    select follower, unnest(friends) as friend from friends_a
    union all select follower, friend from friendships;

--unpacs friends_a
create view friendships_follower_to_friend_ordered_desc as 
    (select follower, friend from friendships order by id desc)
    union all
    (select follower, unnest(friends) as friend from friends_a order by id desc)
    ;




-- Return true or false if the friendship exists
create or replace function followship_exists(follower integer, friend integer)
returns boolean
language sql as $$
    (select true from friendships where follower = $1 and friend = $2)
    union all
    (select true from friends_a where follower = $1 and friends @> ARRAY[$2])
    union all
    (select false)
    limit 1
    ;
$$;

create or replace function num_friends(follower integer)
RETURNS int8
LANGUAGE SQL AS $$
    select sum(c)::int8 from ( 
        (select array_length(friends,1) as c from friends_a where follower = $1)
        union all
        (select count(*) as c from friendships where follower = $1)
    ) as foo;
$$;


create or replace function num_followers(friend integer)
RETURNS int8
LANGUAGE SQL AS $$
    select sum(c)::int8 from ( 
        (select array_length(followers,1) as c from followers_a where friend = $1)
        union all
        (select count(*) as c from friendships where friend = $1)
    ) as foo;
$$;




create or replace function last_n_friends(follower integer, n integer)
RETURNS TABLE (friend integer)
LANGUAGE SQL AS $$
    select friend from friendships_follower_to_friend_ordered_desc where follower = $1 limit $2
$$;

-- Populate followers_a with data from friendships
CREATE OR REPLACE FUNCTION delete_friendship(follower integer, friend integer) RETURNS void AS $$
DECLARE
BEGIN
    delete from friendships where friendships.follower = $1 and friendships.friend = $2;

    update followers_a
        set followers = followers - $1::int
        where followers_a.friend = $2 and followers @> ARRAY[$1];

    update friends_a
        set friends = friends - $2::int
        where friends_a.follower = $1 and friends @> ARRAY[$2];
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cleanup ()
RETURNS TABLE (id integer, follower integer, friend integer)
LANGUAGE SQL AS $$
    DELETE from friendships returning id, follower, friend;
$$;



-- Populate followers_a with data from friendships
CREATE OR REPLACE FUNCTION move_friends() RETURNS void AS $$
DECLARE
BEGIN
--begin;


    -- COPY TO A NEW TEMP TABLE where we have batches.
    create temporary table batch_followers as
    select f.follower, f.friend, f.id,
            trunc(((rank() OVER
                              (partition by f.friend order by f.id)) +
                        COALESCE(array_length(foa.followers,1), 100) - 1) / 100.0) as follower_batch,
            trunc(((rank() OVER
                          (partition by f.follower order by f.id)) +
                    COALESCE(array_length(ffa.friends,1), 100) - 1) / 100.0) as friend_batch

            from cleanup() as f
            left outer join followers_a as foa on (foa.friend = f.friend)
            left outer join friends_a as ffa on (ffa.follower = f.follower)
            where (ffa.append_frozen is not true and foa.append_frozen is not true);

    create index bfo_id  on batch_followers(id);
    create index bfo_fid on batch_followers(follower);
    create index bff_fid on batch_followers(friend);
    create index bfo_bid on batch_followers(follower_batch);
    create index bff_bid on batch_followers(friend_batch);
            
    
    analyze batch_followers;
    
    -- Populate followers_a
    update followers_a as fs
        set followers =  af.followers || fs.followers
        from (
            select array_agg(follower order by id desc) as followers, friend
                from batch_followers
                where follower_batch = 0
                group by friend ) as af
        where af.friend = fs.friend
              and fs.append_frozen is false;

    -- Insert the ones that didn't exist before (and the update didn't do)
    insert into followers_a (followers, friend)
        select array_agg(follower order by id desc), friend as followers
        from batch_followers as bf
        where bf.follower_batch <> 0
        group by friend, bf.follower_batch
        order by bf.follower_batch;
        --where friend in ((select friend from followers_a where append_frozen is false) except (select friend from followers_a where append_frozen is false))
        --where friend not in (select friend from followers_a where append_frozen is false intersect (select friend from friendships_temp))

    -- Freeze the rows that have grown too big
    update followers_a
        set append_frozen = true
        where array_length(followers, 1) >= 100
              and append_frozen is false;

    -- Populate followers_a
    update friends_a as fs
        set friends = af.friends || fs.friends
        from (
            select array_agg(friend order by id desc) as friends, follower
                from batch_followers
                where friend_batch = 0
                group by follower) as af
        where af.follower = fs.follower
              and fs.append_frozen is false;


    -- Insert the ones that didn't exist before (and the update didn't do)
    insert into friends_a  (follower, friends)
        select  follower as followers, array_agg(friend order by id desc)
        from batch_followers as bf
        where bf.friend_batch <> 0
        --where follower in ((select follower from friendships_temp) except (select follower from friends_a where append_frozen is false))
        --where follower not in (select follower from friends_a where append_frozen is false intersect (select follower from friendships_temp))
        group by follower, bf.friend_batch
        order by bf.friend_batch;
        

    -- Freeze the rows that have grown too big
    update friends_a
        set append_frozen = true
        where array_length(friends, 1) >= 100
              and append_frozen is false;

    -- Populate friends_a
    drop table batch_followers cascade;
--commit;
    
END;
$$ LANGUAGE plpgsql;

/*
-- some test data

--populate some data to copy over
copy friendships (follower, friend) from stdin;
1   1
2   2
1   2
2   1
2   3
2   4
2   5
1   3
4   1
3   1
\.

-- move the data over
select move_friends();


insert into friendships (follower, friend) select generate_series(100,200), 1;
copy friendships (follower, friend) from stdin;
1   6
2   7
1   2
2   8
2   66
2   44
2   33
21  3
44  1
\.

select move_friends();

insert into friendships (follower, friend) select generate_series(200,300), 1;
copy friendships (follower, friend) from stdin;
1   6
2   7
1   2
2   8
2   66
2   44
2   33
21  3
44  1
\.

select move_friends();

insert into friendships (follower, friend) select 1, generate_series(200,300);
select * from friendships_friend_to_follower;
*/
