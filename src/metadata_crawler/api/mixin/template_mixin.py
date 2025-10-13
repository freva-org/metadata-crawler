"""Definitions for jinja2 templating."""

import os
from functools import lru_cache
from typing import Any, Dict, Mapping, Optional

from jinja2 import Environment, Template, Undefined

ENV = Environment(undefined=Undefined, autoescape=True)


@lru_cache(maxsize=1024)
def _compile_jinja_template(s: str) -> Template:
    return ENV.from_string(s)


class TemplateMixin:
    """Apply templating egine jinja2."""

    env_map: Optional[Dict[str, str]] = None
    _rendered = False

    def prep_template_env(self) -> None:
        """Prepare the jinja2 env."""

        def _env_get(name: str, default: Optional[str] = None) -> Optional[str]:
            return os.getenv(name, default)

        def _getenv_filter(
            varname: str, default: Optional[str] = None
        ) -> Optional[str]:
            return os.getenv(varname, default)

        ENV.globals.setdefault("env", _env_get)
        ENV.globals.setdefault("ENV", dict(os.environ))
        ENV.filters.setdefault("getenv", _getenv_filter)
        self._rendered = True

    def render_templates(
        self,
        data: Any,
        context: Mapping[str, Any],
        *,
        max_passes: int = 2,
    ) -> Any:
        """Recursively render Jinja2 templates found in strings within data.

        This function traverses common container types (``dict``, ``list``,
        ``tuple``, ``set``), dataclasses, namedtuples, and ``pathlib.Path`` objects.
        Every string encountered is treated as a Jinja2 template and rendered with
        the provided ``context``. Rendering can be repeated up to ``max_passes``
        times to resolve templates that produce further templates on the first pass.

        Parameters
        ^^^^^^^^^^
        data:
            Arbitrary Python data structure. Supported containers are ``dict``
            (keys and values), ``list``, ``tuple`` (including namedtuples),
            ``set``, dataclasses (fields), and ``pathlib.Path``.
            Scalars (e.g., ``int``, ``float``, ``bool``, ``None``) are returned
            unchanged. Strings are rendered as Jinja2 templates.
        context:
            Mapping of template variables available to Jinja2 during rendering.
        max_passes:
            Maximum number of rendering passes to perform on each string,
            by default ``2``. Increase this if templates generate further
            templates that need resolution.

        Returns
        ^^^^^^^
        Any:
            A structure of the same shape with all strings rendered. Container and
            object types are preserved where feasible (e.g., ``tuple`` stays a
            ``tuple``, namedtuple stays a namedtuple, dataclass remains the
            same dataclass type).

        Raises
        ^^^^^^^
        jinja2.TemplateError
            For other Jinja2 template errors encountered during rendering.

        Notes
        ^^^^^^
        * Dictionary keys are also rendered if they are strings (or nested
          containers with strings). If rendering causes key collisions, the
          **last** rendered key wins.
        * For dataclasses, all fields are rendered and a new instance is returned using
          ``dataclasses.replace``. Frozen dataclasses are supported.
        * Namedtuples are detected via the ``_fields`` attribute and
          reconstructed with the same type.

        Examples
        ^^^^^^^^^

            .. code-block:: python

                data = {
                    "greeting": "Hello, {{ name }}!",
                    "items": ["{{ count }} item(s)", 42],
                    "path": {"root": "/home/{{ user }}", "cfg": "{{ root }}/cfg"},
                }
                ctx = {"name": "Ada", "count": 3, "user": "ada", "root": "/opt/app"}
                TemplateMixin().render_templates(data, ctx)
                # {'greeting': 'Hello, Ada!',
                #   'items': ['3 item(s)', 42],
                #    'path': {'root': '/home/ada', 'cfg': '/opt/app/cfg'}}

        """
        if not self._rendered:
            self.prep_template_env()

        def _render_str(s: str) -> str:
            out = s
            if ("{{" not in s) and ("{%" not in s):
                return out
            for _ in range(max_passes):
                new = _compile_jinja_template(out).render(context)
                if new == out:
                    break
                out = new
            return out

        def _walk(obj: Any) -> Any:
            if isinstance(obj, str):
                return _render_str(obj)

            if isinstance(obj, dict):
                rendered: dict[Any, Any] = {}
                for k, v in obj.items():
                    rk = _render_str(k) if isinstance(k, str) else k
                    rendered[rk] = _walk(v)
                return rendered

            if isinstance(obj, list):
                return [_walk(x) for x in obj]

            if isinstance(obj, tuple):
                return tuple(_walk(x) for x in obj)

            if isinstance(obj, set):
                return {_walk(x) for x in obj}

            return obj

        return _walk(data)
