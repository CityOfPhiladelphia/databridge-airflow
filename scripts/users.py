#!/usr/bin/env python

import click
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

@click.group()
def main():
    pass

@main.command()
@click.argument('username')
@click.argument('email')
@click.argument('password')
@click.option('--superuser/--no-superuser', default=False)
def adduser(username, email, password, superuser):
    user = PasswordUser(models.User())
    user.username = username
    user.email = email
    user.password = password
    if superuser:
        user.superuser = True
    session = settings.Session()
    session.add(user)
    session.commit()
    session.close()

if __name__ == '__main__':
    main()
