#include "postgres.h"
#include "utils/array.h"

PG_FUNCTION_INFO_V1(array_agg_transfn_not_null);
PG_FUNCTION_INFO_V1(array_agg_finalfn_default_empty);


/*
 * ARRAY_AGG aggregate function
 */
Datum
array_agg_transfn_not_null(PG_FUNCTION_ARGS)
{
	Oid			arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
	MemoryContext aggcontext;
	ArrayBuildState *state;
	Datum		elem;

	if (arg1_typeid == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "array_agg_transfn_not_null called in non-aggregate context");
	}

	state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);

	if (!PG_ARGISNULL(1))
	{
		elem = PG_ARGISNULL(1) ? (Datum) 0 : PG_GETARG_DATUM(1);
		state = accumArrayResult(state,
								 elem,
								 PG_ARGISNULL(1),
								 arg1_typeid,
								 aggcontext);
	}

	/*
	 * The transition type for array_agg() is declared to be "internal", which
	 * is a pass-by-value type the same size as a pointer.	So we can safely
	 * pass the ArrayBuildState pointer through nodeAgg.c's machinations.
	 */
	PG_RETURN_POINTER(state);
}

Datum
array_agg_finalfn_default_empty(PG_FUNCTION_ARGS)
{
	Datum		result;
	ArrayBuildState *state;
	int			dims[1];
	int			lbs[1];

	/*
	 * Test for null before Asserting we are in right context.	This is to
	 * avoid possible Assert failure in 8.4beta installations, where it is
	 * possible for users to create NULL constants of type internal.
	 */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();		/* returns null iff no input values */

	/* cannot be called directly because of internal-type argument */
	Assert(AggCheckCallContext(fcinfo, NULL));

	state = (ArrayBuildState *) PG_GETARG_POINTER(0);

	dims[0] = state->nelems;
	lbs[0] = 1;

	/*
	 * Make the result.  We cannot release the ArrayBuildState because
	 * sometimes aggregate final functions are re-executed.  Rather, it
	 * is nodeAgg.c's responsibility to reset the aggcontext when it's
	 * safe to do so.
	 */
	result = makeMdArrayResult(state, 1, dims, lbs,
							   CurrentMemoryContext,
							   false);

	PG_RETURN_DATUM(result);
}


PG_MODULE_MAGIC;

/*
 * From pgsql/contrib/intarray/_int_op.c
 */

/* dimension of array */
#define NDIM 1

#define ARRPTR(x)  ( (int4 *) ARR_DATA_PTR(x) )
#define ARRNELEMS(x)  ArrayGetNItems(ARR_NDIM(x), ARR_DIMS(x))

/* reject arrays we can't handle; but allow a NULL or empty array */
#define CHECKARRVALID(x) \
	do { \
		if (x) { \
			if (ARR_NDIM(x) != NDIM && ARR_NDIM(x) != 0) \
				ereport(ERROR, \
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), \
						 errmsg("array must be one-dimensional"))); \
			if (ARR_HASNULL(x)) \
				ereport(ERROR, \
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), \
						 errmsg("array must not contain nulls"))); \
		} \
	} while(0)

#define ARRISVOID(x)  ((x) == NULL || ARRNELEMS(x) == 0)

typedef ArrayType *(*formarray) (ArrayType *, ArrayType *);

PG_FUNCTION_INFO_V1(followship_intarray_del_elem);
Datum		followship_intarray_del_elem(PG_FUNCTION_ARGS);

ArrayType *
resize_intArrayType(ArrayType *a, int num)
{
	int			nbytes = ARR_OVERHEAD_NONULLS(NDIM) + sizeof(int) * num;

	if (num == ARRNELEMS(a))
		return a;

	a = (ArrayType *) repalloc(a, nbytes);

	SET_VARSIZE(a, nbytes);
	*((int *) ARR_DIMS(a)) = num;
	return a;
}

Datum
followship_intarray_del_elem(PG_FUNCTION_ARGS)
{
	ArrayType  *a = (ArrayType *) DatumGetPointer(PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(0)));
	int32		elem = PG_GETARG_INT32(1);
	int32		c;
	int32	   *aa;
	int32		n = 0,
				i;

	CHECKARRVALID(a);
	if (!ARRISVOID(a))
	{
		c = ARRNELEMS(a);
		aa = ARRPTR(a);
		for (i = 0; i < c; i++)
		{
			if (aa[i] != elem)
			{
				if (i > n)
					aa[n++] = aa[i];
				else
					n++;
			}
		}
		a = resize_intArrayType(a, n);
	}
	PG_RETURN_POINTER(a);
}

