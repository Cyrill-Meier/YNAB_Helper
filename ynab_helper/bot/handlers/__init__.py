"""Bot handlers, one module per feature (DESIGN §8.3).

Every handler takes a :class:`ynab_helper.bot.context.Request` whose sender already passed
the authorization gate for this command or button. Handlers do fast work inline and hand
anything that talks to YNAB, a blockchain API or Telegram's file server to
:meth:`ynab_helper.bot.app.Bot.start_job`.
"""
