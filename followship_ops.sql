CREATE OR REPLACE FUNCTION followship_intarray_del_elem(_int4, int4)
RETURNS _int4
AS '/Users/mike/Desktop/followships/followship_ops'
LANGUAGE C STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION array_agg_transfn_not_null(internal, anyelement)
RETURNS internal
AS '/Users/mike/Desktop/followships/followship_ops'
LANGUAGE C IMMUTABLE;

-- :(
CREATE AGGREGATE array_accum (anyelement) (
    sfunc = array_agg_transfn_not_null,
    stype = internal,
    finalfunc = array_agg_finalfn
);


