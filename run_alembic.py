import subprocess


def main(direction, revision):
    # Replace file path with your alembic install location
    path = "\\Users\\Off.Network.User8\\AppData\\Local\\Programs\\Python\\Python37\\Scripts"
    subprocess.run(["alembic", direction, revision], cwd=path)


main("upgrade", "base_migration_model")
# The direction can be 'upgrade'  or 'downgrade'
# The basic revisions for create/drop are 'empty_database' and 'base_migration_model'
