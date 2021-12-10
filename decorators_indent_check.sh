if grep -E -i -rnw "^[[:space:]]+@(transformation|notebook_function)" --include="*.py" src; then
  echo "It is always a bad idea to indent decorated functions to use them in ifs, for loops, ... You can do better than that!"
  exit 1
else
  echo "All good."
fi
