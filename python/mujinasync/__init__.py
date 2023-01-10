# -*- coding: utf-8 -*-

from .version import __version__ # noqa: F401

try:
    import mujincommon.i18n
    ugettext, ungettext = mujincommon.i18n.GetDomain('mujinasyncpy').GetTranslationFunctions()
except ImportError:
    def ugettext(message):
        return message

    def ungettext(singular, plural, n):
        return singular if n == 1 else plural

_ = ugettext

