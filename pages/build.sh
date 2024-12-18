docker run --rm \
    --network host \
    --volume="$(pwd):/srv/jekyll:Z" \
    -it jekyll/jekyll:3.8.6 \
    jekyll build --incremental
