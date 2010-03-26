===========
Followships
===========

Introduction
============

This is a module for maintaining followships.

Requirements
============
Postgresql 9.0 (Alpha) (and 8.5 possibly works as well)


Installation
============


Let's make sure your postgres is running.  If you haven't initialized a
database.  Set the environment variable ``PGDATA`` to a path where you want your
database install to go and set ``PGDATABASE`` to ``postgres``. Then run
(assuming you set your path to the install directory)::

  initdb # creates the database
  pg_ctl start # starts up postgres

After Postgresql 9.0 is installed, you must build the the external c modules and
install them.  Currently, you will have to modify the Makefile.

If you are on a mac just change ``-I/users/mike/apps/include/postgresql/server``
to the include path wherever your Postgres 9.0 install is.  Otherwise, refer to 
`the postgres manual <http://www.postgresql.org/docs/8.4/static/xfunc-c.html#DFUNC>`_
to see how to build for your system.

Also, edit ``followship_ops.sql`` and change the occurrences of
``/Users/mike/Desktop/followships/`` to your working directory.

After run::

  make
  psql -f followship_ops.sql    # install the functions we need

This should be the only time we need to do this.

To install the schema, run::

  psql -f followships_2.sql


Ok, our schema is installed.

Operations
==========

Everything we created from followships_2.sql is in the schema followship.  To be
able to access these, one must either prefix the function calls and table names
with ``followship.`` or call ``set search_path to followship, public;`` at the
beginning of your psql session.



Inserting
---------
Just insert friend, follower into the ``followships`` table like normal.

Data Loading
------------

``select move_friends()``
  Move friends starts the operation to move rows from ``followships`` to
  ``followship_rollups``.  This does not block any inserts or reads.  All the
  current rows in followships are locked with ``SELECT FOR UPDATE`` and will be
  deleted after the operation is done;

``select bulk_load_friends(filepath text)``
  This is similar to ``move_friends`` but is for bulk loading data while not
  online.  **DO NOT USE THIS FUNCTION IF THERE IS ANY DATA IN FOLLOWSHIPS**.

  You pass this function an absolute path to a csv file in the format
  ``follower, friend`` and it will roll up the data.  This bypasses indexes in
  the ``followships`` table and deleting the rows (dropping the temporary table
  it makes is much quicker.  ``run.sh`` calls this function.

Querying
--------

A few functions are built in.  These include:

``select num_friends(user_id)``
  Returns the number of friends that user has

``select num_followers(user_id)``
  Returns the number of followers that user has

``select has_friend(user_id integer, friend_id integer)``
  returns ``true`` if ``user_id`` is following ``friend_id`` otherwise returns
  ``false``

``select has_follower(user_id integer, follower_id integer)``
  returns ``true`` if ``follower_id`` is following ``user_id`` otherwise returns
  ``false``. (you can find the same information with has_friend)

``select * from last_n_friends(user_id, n)``
  returns the last ``n`` users ``user_id`` followed in (first row is most
  recent)

``select * from last_n_followers(user_id, n)``
  returns the last ``n`` users ``user_id`` has been followed by (first row is
  most recent)

Deleting
--------

``select delete_followship(follower integer, friend integer)``
  Does what it says.  Cleans up the data in the rollups and whatnot too



