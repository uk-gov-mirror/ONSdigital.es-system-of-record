import subprocess


def main(direction, revision):
    """Code to run Alembic upgrade and downgrade commands.
    Parameters:
      direction (String):The word upgrade or downgrade.
      revision (String):Name of the migration file to be moved to.
    """

    # Replace file path with your alembic install location
    path = "~/PycharmProjects/es-system-of-record"
    subprocess.run(["alembic", direction, revision], cwd=path)


main("upgrade", "base_migration_model")
# The direction can be 'upgrade'  or 'downgrade'
# The basic revisions for create/drop are 'empty_database'
# and 'base_migration_model'
