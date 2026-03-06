import click
import clickhouse_migrator.tool as clickhouse_migration_tool
from loguru import logger


@click.group()
def migrator():
    pass


@migrator.command()
@click.option('--configfile')
def clean(configfile):
    settings = clickhouse_migration_tool.Settings(configfile)
    logger.info(f'Loaded {settings.env} config file.')
    tool = clickhouse_migration_tool.MigrationTool(settings)
    tool.clean()
    logger.info('Cleanup completed')


@migrator.command()
@click.option('--configfile')
def migrate(configfile):
    settings = clickhouse_migration_tool.Settings(configfile)
    logger.info(f'Loaded {settings.env} config file.')
    tool = clickhouse_migration_tool.MigrationTool(settings)
    tool.migrate()
    logger.info('Migration completed')


@migrator.command()
@click.option('--configfile')
def create_db(configfile):
    settings = clickhouse_migration_tool.Settings(configfile)
    logger.info(f'Loaded {settings.env} config file.')
    tool = clickhouse_migration_tool.MigrationTool(settings)
    tool.create_db()
    logger.info('Creation completed')


if __name__ == '__main__':
    migrator()
