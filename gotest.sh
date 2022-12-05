for x in . cmd/bsearch cmd/bsearch_index; do
    pushd "$x"
    go test
    popd
done
