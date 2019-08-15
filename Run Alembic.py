import subprocess


def main(direction, revision):
    # Replace file path with your alembic install location
    path = "\\Users\\Off.Network.User8\\AppData\\Local\\Programs\\Python\\Python37\\Scripts"
    subprocess.run(["alembic", direction, revision], cwd=path)


main("upgrade", "7e4c6b9e5a5d")
# The direction can be 'upgrade'  or 'downgrade'
# The basic revisions for create/drop are 'Empty_Database' and '7e4c6b9e5a5d'
