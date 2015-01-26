# Remove building leftovers
rm -rf build dist *egg-info

# Remove pyc files
find . -type f -name *pyc -exec rm {} +
