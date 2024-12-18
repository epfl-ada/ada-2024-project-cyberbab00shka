docker run --rm \
    -p 4000:4000 \
    -p 35729:35729 \
    -v bundle-cache:/bundle \
    --volume="$(pwd):/srv/jekyll:Z" \
    -it jekyll/jekyll:3.8.6 \
    jekyll serve --livereload --host 0.0.0.0
