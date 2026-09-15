---
name: browser-policy
description: Browser execution policy for OpenCode running in WSL2 and controlling the dedicated authenticated Windows Chrome instance. Use this skill whenever a task requires browser interaction, authenticated websites, web UI automation, or manual login/2FA/CAPTCHA handoff.
compatibility: opencode
---

# Browser Policy

This skill defines HOW browser automation is deployed in this environment.

It does not define generic Playwright commands.
Those are provided by the `playwright-cli` skill.

## Required dependency

Before executing the first browser operation:

1. Load the `playwright-cli` skill.
2. Follow its instructions for all Playwright CLI interactions.

Do not duplicate or replace the generic Playwright CLI workflow defined
by that skill.

## Browser architecture

The browser is a real Google Chrome instance running on the Windows host.

OpenCode and `playwright-cli` run inside WSL2.

The browser exposes Chrome DevTools Protocol at:

    http://127.0.0.1:9222

The standard Playwright CLI session name is:

    win-chrome
	
The environment should provide:

    PLAYWRIGHT_CLI_SESSION=win-chrome
    BROWSER_CDP_ENDPOINT=http://127.0.0.1:9222

## Connection policy

Before browser interaction, check whether the CDP endpoint is available:

    curl -fsS "${BROWSER_CDP_ENDPOINT}/json/version"

If an attached Playwright CLI session is already available, reuse it.

Otherwise attach with:

    playwright-cli attach \
      --cdp="${BROWSER_CDP_ENDPOINT}" \
      -s=win-chrome

Use the same session for the entire business workflow.

## Authentication policy

For authenticated websites:

- Reuse the dedicated Windows Chrome profile and its existing login state.
- Do not launch a fresh Playwright-managed browser unless explicitly required.
- Do not automate CAPTCHA, slider verification, security challenges, or 2FA.
- If human verification is required, allow the user to complete it in the
  visible Windows Chrome browser.
- Continue browser automation only after authentication succeeds.

Never attempt to bypass anti-bot or human-verification mechanisms.

## Interaction policy

Prefer:

    snapshot -> inspect refs -> interact -> snapshot/verify

Prefer semantic element refs over coordinate-based clicking.

Use screenshots or vision only when visual inspection is actually needed,
such as canvas, charts, maps, graphical editors, or layout validation.

Do not use arbitrary sleeps as the primary synchronization method.
Wait for observable page state instead.

## Browser ownership

The Windows Chrome process is externally owned.

Never use `playwright-cli close` to terminate an externally attached Chrome
session.

When browser automation is finished, use:

    playwright-cli -s=win-chrome detach

Detaching must leave Windows Chrome running.

## Safety

Treat the attached Chrome session as authenticated user access.

Do not:
- expose cookies or credentials,
- navigate to unrelated sensitive sites,
- modify account security settings,
- perform irreversible business actions unless required by the business skill.

When the business skill requires a consequential action, follow its
confirmation and verification rules.
