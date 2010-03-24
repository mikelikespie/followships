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

create view all_followers as
    select array_agg(follower) as followers, friend
        from friendships group by friend;
    
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
create index followers_a_expanded on friends_a using gin (followers);

create view all_friends as
    select follower, array_agg(friend) as friends
        from friendships group by follower;

-- Unpacsk and unions with the 
create view friendships_friend_to_follower as 
    select unnest(followers) as follower, friend from followers_a
    union all select follower, friend from friendships;

-- does same, but ordered
create view friendships_friend_to_follower_ordered as 
    (select unnest(followers) as follower, friend from followers_a order by id)
    union all (select follower, friend from friendships order by id);


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
create index friends_a_expanded on friends_a using gin (friends);

--unpacs friends_a
create view friendships_follower_to_friend as 
    select follower, unnest(friends) as friend from friends_a
    union all select follower, friend from friendships;

--unpacs friends_a
create view friendships_follower_to_friend_ordered as 
    (select follower, unnest(friends) as friend from friends_a order by id)
    union all (select follower, friend from friendships order by id);




CREATE OR REPLACE FUNCTION cleanup ()
RETURNS TABLE (follower integer, friend integer)
LANGUAGE SQL AS $$
    DELETE from friendships returning follower, friend;
$$;


-- Populate followers_a with data from friendships
CREATE FUNCTION move_friends() RETURNS void AS $$
DECLARE
BEGIN
--begin;
    create temporary table friendships_temp
        as select * from cleanup();

	create index ft_i on friendships_temp (friend);
	create index ft_i2 on friendships_temp (follower);

    -- Populate followers_a
    update followers_a as fs
        set followers = fs.followers || af.followers
        from (
            select array_agg(follower) as followers, friend
                from friendships_temp
                group by friend) as af
        where af.friend = fs.friend
              and fs.append_frozen is false;

    -- Insert the ones that didn't exist before (and the update didn't do)
    insert into followers_a  (followers, friend)
        select array_agg(follower), friend as followers
        from friendships_temp, 
			((select friend as ff from followers_a where append_frozen is false) except (select friend from followers_a where append_frozen is false)) as ftof
		where friend = ftof.ff
        --where friend in ((select friend from followers_a where append_frozen is false) except (select friend from followers_a where append_frozen is false))
        --where friend not in (select friend from followers_a where append_frozen is false intersect (select friend from friendships_temp))
        group by friend;

    -- Freeze the rows that have grown too big
    update followers_a
        set append_frozen = true
        where array_length(followers, 1) >= 100
              and append_frozen is false;

    -- Populate followers_a
    update friends_a as fs
        set friends = array_cat(fs.friends, af.friends)
        from (
            select array_agg(friend) as friends, follower
                from friendships_temp
                group by follower) as af
        where af.follower = fs.follower
              and fs.append_frozen is false;




    -- Insert the ones that didn't exist before (and the update didn't do)
    insert into friends_a  (follower, friends)
        select  follower as followers, array_agg(friend)
        from friendships_temp,
			((select follower as ff from friendships_temp) except (select follower from friends_a where append_frozen is false)) as ftof
		where follower = ftof.ff
        --where follower in ((select follower from friendships_temp) except (select follower from friends_a where append_frozen is false))
        --where follower not in (select follower from friends_a where append_frozen is false intersect (select follower from friendships_temp))
        group by follower;

    -- Freeze the rows that have grown too big
    update friends_a
        set append_frozen = true
        where array_length(friends, 1) >= 100
              and append_frozen is false;

    -- Populate friends_a
    drop table friendships_temp cascade;
--commit;
    
END;
$$ LANGUAGE plpgsql;


-- some test data

--populate some data to copy over
copy friendships (follower, friend) from stdin;
1	1
2	2
1	2
2	1
2	3
2	4
2	5
1	3
4	1
3	1
\.

-- move the data over
select move_friends();


insert into friendships (follower, friend) select generate_series(100,200), 1;
copy friendships (follower, friend) from stdin;
1	6
2	7
1	2
2	8
2	66
2	44
2	33
21	3
44	1
\.

select move_friends();

insert into friendships (follower, friend) select generate_series(200,300), 1;
copy friendships (follower, friend) from stdin;
1	6
2	7
1	2
2	8
2	66
2	44
2	33
21	3
44	1
\.

select move_friends();

insert into friendships (follower, friend) select 1, generate_series(200,300);
select * from friendships_friend_to_follower;

