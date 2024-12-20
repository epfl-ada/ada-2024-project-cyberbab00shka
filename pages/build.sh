docker run --rm \
    -p 4000:4000 \
    -p 35729:35729 \
    --volume="$(pwd):/srv/jekyll:Z" \
    -it jekyll/jekyll:3.8.6 \
    jekyll serve --livereload --incremental --host 0.0.0.0

