from __future__ import absolute_import, unicode_literals
from os import environ, path
from celery import Celery
import features.settings as settings


PROJECT_NAME = path.basename(path.dirname(__file__))
environ.setdefault('DJANGO_SETTINGS_MODULE', '%s.settings' % PROJECT_NAME)

app = Celery(PROJECT_NAME)
app.config_from_object('django.conf:settings')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
