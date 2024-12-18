docker run --rm \
    --network host \
    --volume="$(pwd):/srv/jekyll:Z" \
    -it jekyll/jekyll:latest \
    jekyll build --incremental
