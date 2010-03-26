export PGDATABASE=postgres
for f in $*
do
	echo "copying $f"
	psql -c "set search_path to followship, public; select bulk_load_friends('`pwd`/$f');"
	# psql -c "vacuum analyze;"
done
