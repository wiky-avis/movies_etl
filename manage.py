import click

from etl.common.manager import ETLManager


@click.group()
def cli():
    pass


@cli.command("run")
def run():
    daemon = ETLManager()
    daemon.run()


if __name__ == "__main__":
    cli()
