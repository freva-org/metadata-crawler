"""Named connections for index systems: --server/--url NAME."""

from __future__ import annotations

import base64
import datetime
import ipaddress
import ssl
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, Iterator, List, Optional, Tuple
from urllib.parse import parse_qs, urlsplit

import aiohttp
import pydantic
import pytest
from aiohttp import web

import metadata_crawler.cli as mc_cli
import metadata_crawler.run as mc_run
from metadata_crawler.api.index import BaseIndex
from metadata_crawler.api.stores.mongodb import MongoConnection
from metadata_crawler.api.stores.postgresql import PostgresConnection
from metadata_crawler.connections import ConfigFiles, read_configfiles
from metadata_crawler.ingester.mongo import MongoIndex, _redact
from metadata_crawler.ingester.solr import SolrConnection, SolrIndex

WriteToml = Callable[..., Path]


# ---------------------------------------------------------------------------
# The Solr connection model
# ---------------------------------------------------------------------------


class TestSolrConnection:
    @pytest.mark.parametrize(
        "url, store_uri",
        [
            ("https://solr.example.org:8983", "https://solr.example.org:8983"),
            ("http://solr.example.org:8983/solr", "http://solr.example.org:8983"),
            ("solr.example.org:8983", "http://solr.example.org:8983"),
            ("https://solr.example.org", "https://solr.example.org"),
        ],
    )
    def test_store_uri(self, url: str, store_uri: str) -> None:
        assert SolrConnection.from_url(url).store_uri == store_uri

    def test_credentials_in_the_url_move_to_the_masked_fields(self) -> None:
        conn = SolrConnection.from_url("https://indexer:s3cr3t@h:8983/solr")
        assert conn.username == "indexer"
        assert conn.password is not None
        assert conn.password.get_secret_value() == "s3cr3t"
        assert "s3cr3t" not in repr(conn) and "s3cr3t" not in conn.url
        assert conn.store_uri == "https://h:8983"

    def test_auth_headers(self) -> None:
        conn = SolrConnection.from_url("https://h", user="indexer", passwd="s3cr3t")
        assert conn.auth_headers() == {"Authorization": _basic("indexer", "s3cr3t")}

    def test_no_credentials_no_auth(self) -> None:
        assert SolrConnection.from_url("https://h").auth_headers() == {}

    def test_validating_an_instance_returns_it(self) -> None:
        conn = SolrConnection.from_url("https://h")
        assert SolrConnection.model_validate(conn) is conn

    def test_unknown_keys_are_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            SolrConnection.from_url("https://h", database="solr")

    def test_http_urls_need_the_backend(self) -> None:
        """Without ``backend`` an http URL stays an intake catalogue."""
        cfg = ConfigFiles(
            {
                "solr-prod": {"backend": "solr", "url": "https://h:8983"},
                "cat": {"url": "https://h/cat.yml"},
            }
        )
        assert isinstance(cfg["solr-prod"], SolrConnection)
        assert cfg["cat"].backend == "intake"


# ---------------------------------------------------------------------------
# BaseIndex: target type and the option that takes it
# ---------------------------------------------------------------------------


class NoTargets(BaseIndex):
    """An index system without a connection model."""

    async def index(
        self,
        metadata: Optional[Dict[str, Any]] = None,
        core: Optional[str] = None,
        **kwargs: Any,
    ) -> None: ...

    async def delete(self, **kwargs: Any) -> None: ...


class Unmarked(NoTargets):
    """Has a connection model, but no option is marked to take it."""

    connection = SolrConnection


class TestBaseIndexTarget:
    async def test_target_is_kept(self) -> None:
        conn = SolrConnection.from_url("https://h")
        assert SolrIndex(target=conn).target is conn

    def test_wrong_kind(self) -> None:
        conn = PostgresConnection.from_url("postgresql://h/db")
        with pytest.raises(TypeError, match="needs a solr connection"):
            SolrIndex(target=conn)

    def test_index_system_without_targets(self) -> None:
        with pytest.raises(TypeError, match="doesn't support"):
            NoTargets(target=SolrConnection.from_url("https://h"))

    @pytest.mark.parametrize(
        "cls, method, option",
        [
            (SolrIndex, "index", "server"),
            (SolrIndex, "delete", "server"),
            (MongoIndex, "index", "url"),
            (MongoIndex, "delete", "url"),
            (NoTargets, "index", None),
            (Unmarked, "index", None),
            (SolrIndex, "nothing", None),
        ],
    )
    def test_target_option(self, cls: Any, method: str, option: Optional[str]) -> None:
        assert cls.target_option(method) == option


# ---------------------------------------------------------------------------
# Solr: explicit options win, every request carries the auth
# ---------------------------------------------------------------------------


class TestSolrIndexTarget:
    async def test_server_falls_back_to_the_target(self) -> None:
        solr = SolrIndex(target=SolrConnection.from_url("https://h:8983"))
        assert solr._server(None) == "https://h:8983"
        assert solr._server("other:8983") == "other:8983"

    async def test_without_target(self) -> None:
        solr = SolrIndex()
        assert solr._server(None) == ""
        assert solr._headers == {}


@pytest.fixture()
async def fake_solr() -> AsyncIterator[Tuple[str, List[Dict[str, Any]]]]:
    """A local HTTP server recording what reaches Solr's update handler."""
    requests: List[Dict[str, Any]] = []

    async def update(request: web.Request) -> web.Response:
        requests.append(
            {
                "path": request.path,
                "auth": request.headers.get("Authorization"),
                "body": await request.json(),
            }
        )
        return web.json_response({"responseHeader": {"status": 0}})

    app = web.Application()
    app.router.add_post("/solr/{core}/update/json", update)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    try:
        yield f"http://127.0.0.1:{port}", requests
    finally:
        await runner.cleanup()


def _basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


@pytest.fixture()
def solr_plugin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mc_run, "load_plugins", lambda group: {"solr": SolrIndex})


class TestSolrRequests:
    async def test_delete_with_a_connection(
        self, fake_solr: Tuple[str, List[Dict[str, Any]]], solr_plugin: None
    ) -> None:
        url, requests = fake_solr
        conn = SolrConnection.from_url(url, username="indexer", password="s3cr3t")
        await mc_run.async_delete("solr", server=conn, facets=[("project", "obs")])
        assert {r["path"] for r in requests} == {
            "/solr/files/update/json",
            "/solr/latest/update/json",
        }
        assert all(r["auth"] == _basic("indexer", "s3cr3t") for r in requests)
        assert requests[0]["body"] == {"delete": {"query": "project:obs"}}

    async def test_delete_with_an_address(
        self, fake_solr: Tuple[str, List[Dict[str, Any]]], solr_plugin: None
    ) -> None:
        url, requests = fake_solr
        await mc_run.async_delete("solr", server=url, facets=[("project", "obs")])
        assert len(requests) == 2
        assert all(r["auth"] is None for r in requests)


# ---------------------------------------------------------------------------
# MongoDB: the same connection model as a MongoDB catalogue
# ---------------------------------------------------------------------------


class TestMongoIndexTarget:
    @pytest.fixture()
    def conn(self) -> MongoConnection:
        return MongoConnection.from_url(
            "mongodb://mongo.example.org:27018/search",
            username="indexer",
            password="s3cr3t",
            tls=True,
        )

    async def test_connection(self, conn: MongoConnection) -> None:
        mongo = MongoIndex(target=conn)
        db = await mongo._prep_db_connection(None, None)
        parts = urlsplit(mongo.uri)
        assert (parts.hostname, parts.port) == ("mongo.example.org", 27018)
        assert (parts.username, parts.password) == ("indexer", "s3cr3t")
        assert parse_qs(parts.query)["tls"] == ["true"]
        assert db.name == "search"
        await mongo.close()

    async def test_explicit_database_wins(self, conn: MongoConnection) -> None:
        mongo = MongoIndex(target=conn)
        db = await mongo._prep_db_connection("other", None)
        assert db.name == "other"
        await mongo.close()

    async def test_explicit_url_wins(self, conn: MongoConnection) -> None:
        mongo = MongoIndex(target=conn)
        await mongo._prep_db_connection(None, "mongodb://elsewhere:27017")
        assert urlsplit(mongo.uri).hostname == "elsewhere"
        assert urlsplit(mongo.uri).password is None
        await mongo.close()

    async def test_defaults_without_target(self) -> None:
        mongo = MongoIndex()
        db = await mongo._prep_db_connection(None, "mongodb://h:27017")
        assert db.name == "metadata"
        await mongo.close()

    async def test_new_url_gets_a_new_client(self) -> None:
        """close() must really close, and the next URL must not reuse it."""
        mongo = MongoIndex()
        await mongo._prep_db_connection(None, "mongodb://first:27017")
        first = mongo.client
        await mongo._prep_db_connection(None, "mongodb://second:27017")
        assert mongo.client is not first
        assert urlsplit(mongo.uri).hostname == "second"
        await mongo.close()

    async def test_context_closes_the_client(self) -> None:
        async with MongoIndex() as mongo:
            await mongo._prep_db_connection(None, "mongodb://h:27017")
            assert mongo._client is not None
        assert mongo._client is None

    @pytest.mark.parametrize(
        "uri, shown",
        [
            (
                "mongodb://u:s3cr3t@h:27017/db?tls=true",
                "mongodb://u:***@h:27017/db?tls=true",
            ),
            ("mongodb://h:27017/db", "mongodb://h:27017/db"),
        ],
    )
    def test_logged_uri_is_redacted(self, uri: str, shown: str) -> None:
        assert _redact(uri) == shown


# ---------------------------------------------------------------------------
# async_call hands the connection to the constructor
# ---------------------------------------------------------------------------


class Recorded:
    """Index plugin recording how it was constructed and called."""

    init: Dict[str, Any] = {}
    calls: List[Dict[str, Any]] = []


def _recording(cls: type) -> type:
    class Recording(cls):  # type: ignore[valid-type, misc]
        def __init__(self, **kwargs: Any) -> None:
            Recorded.init = kwargs
            super().__init__(**kwargs)

        async def index(self, **kwargs: Any) -> None:
            Recorded.calls.append(kwargs)

        async def delete(self, **kwargs: Any) -> None:
            Recorded.calls.append(kwargs)

        @classmethod
        def target_option(cls, method: str) -> Optional[str]:
            return getattr(cls.__mro__[1], "target_option")(method)

    return Recording


@pytest.fixture()
def recorded(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    Recorded.init, Recorded.calls = {}, []
    monkeypatch.setattr(
        mc_run,
        "load_plugins",
        lambda group: {"solr": _recording(SolrIndex), "plain": _recording(NoTargets)},
    )
    yield


class TestAsyncCall:
    async def test_connection_becomes_the_target(self, recorded: None) -> None:
        conn = SolrConnection.from_url("https://h:8983")
        await mc_run.async_delete("solr", server=conn, facets=[("a", "b")])
        assert Recorded.init["target"] is conn
        assert "server" not in Recorded.calls[0]
        assert Recorded.calls[0]["facets"] == [("a", "b")]

    async def test_addresses_stay_with_the_method(self, recorded: None) -> None:
        await mc_run.async_delete("solr", server="localhost:8983")
        assert "target" not in Recorded.init
        assert Recorded.calls[0]["server"] == "localhost:8983"

    async def test_plugins_without_targets_get_no_target_argument(
        self, recorded: None
    ) -> None:
        await mc_run.async_delete("plain", server="x")
        assert "target" not in Recorded.init


# ---------------------------------------------------------------------------
# Command line: --server/--url accept connection names
# ---------------------------------------------------------------------------


class ApiRecorder:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append({"args": args, **kwargs})

    @property
    def last(self) -> Dict[str, Any]:
        assert self.calls, "the API function was not called"
        return self.calls[-1]


@pytest.fixture()
def config(config_home: Path, write_toml: WriteToml) -> List[str]:
    conns = write_toml(
        "c.toml",
        """
[prod]
url = "postgresql://db.example.org/metadata"

[solr-prod]
backend = "solr"
url = "https://solr.example.org:8983"

[mongo-search]
url = "mongodb://mongo.example.org/search"
""",
        0o644,
    )
    secrets = write_toml(
        "s.toml",
        """
[prod]
username = "ab1234"
password = "pw"

[solr-prod]
username = "indexer"
password = "s3cr3t"

[mongo-search]
username = "m"
password = "mpw"
""",
    )
    return ["--mdc-config", str(conns), "--mdc-secrets", str(secrets)]


@pytest.fixture()
def api(monkeypatch: pytest.MonkeyPatch) -> Dict[str, ApiRecorder]:
    recorders = {"index": ApiRecorder(), "delete": ApiRecorder()}
    for name, recorder in recorders.items():
        monkeypatch.setattr(mc_cli, name, recorder)
    monkeypatch.setattr(
        mc_cli, "load_plugins", lambda ep: {"solr": SolrIndex, "mongo": MongoIndex}
    )
    return recorders


class TestCli:
    def test_server_name(self, config: List[str], api: Dict[str, ApiRecorder]) -> None:
        mc_cli.cli(["solr", "index", "prod", "--server", "solr-prod", *config])
        call = api["index"].last
        assert isinstance(call["server"], SolrConnection)
        assert call["server"].username == "indexer", "secrets are merged"
        assert isinstance(call["metadata_stores"][0], PostgresConnection)
        assert call["index_system"] == "solr"

    def test_server_address(
        self, config: List[str], api: Dict[str, ApiRecorder]
    ) -> None:
        mc_cli.cli(["solr", "index", "prod", "--server", "localhost:8983", *config])
        assert api["index"].last["server"] == "localhost:8983"

    def test_server_url_needs_no_config(
        self, config_home: Path, api: Dict[str, ApiRecorder], tmp_path: Path
    ) -> None:
        cat = tmp_path / "cat.yml"
        cat.write_text("")
        argv = ["solr", "index", str(cat), "--server", "http://h:8983"]
        mc_cli.cli([*argv, "--mdc-config", str(tmp_path / "missing.toml")])
        assert api["index"].last["server"] == "http://h:8983"

    def test_delete(self, config: List[str], api: Dict[str, ApiRecorder]) -> None:
        mc_cli.cli(["solr", "delete", "-sv", "solr-prod", "-f", "a", "b", *config])
        assert isinstance(api["delete"].last["server"], SolrConnection)

    def test_mongo_url_name(
        self, config: List[str], api: Dict[str, ApiRecorder]
    ) -> None:
        mc_cli.cli(["mongo", "index", "prod", "--url", "mongo-search", *config])
        call = api["index"].last
        assert isinstance(call["url"], MongoConnection)
        assert call["database"] is None, "the connection's database is used"

    def test_option_keys_do_not_reach_the_api(
        self, config: List[str], api: Dict[str, ApiRecorder]
    ) -> None:
        mc_cli.cli(["solr", "index", "prod", "--server", "solr-prod", *config])
        assert "target_option" not in api["index"].last
        assert "connection" not in api["index"].last

    def test_help_mentions_connections(
        self, api: Dict[str, ApiRecorder], capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit):
            mc_cli.cli(["solr", "index", "--help"])
        assert "connections.toml" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Solr: token auth and TLS
# ---------------------------------------------------------------------------


class TestSolrAuthAndTls:
    def test_token(self) -> None:
        conn = SolrConnection.from_url("https://h", token="t0k3n")
        assert conn.auth_headers() == {"Authorization": "Bearer t0k3n"}
        assert "t0k3n" not in repr(conn)

    def test_token_and_password_conflict(self) -> None:
        with pytest.raises(pydantic.ValidationError, match="either token"):
            SolrConnection.from_url("https://h", token="t", username="u")

    def test_ca_file_needs_verification(self, tmp_path: Path) -> None:
        with pytest.raises(pydantic.ValidationError, match="no effect"):
            SolrConnection.from_url(
                "https://h", ca_file=str(tmp_path / "ca.pem"), verify_ssl=False
            )

    def test_ssl_settings(self, tls: Dict[str, Path]) -> None:
        assert SolrConnection.from_url("https://h").ssl() is True
        assert SolrConnection.from_url("https://h", verify_ssl=False).ssl() is False
        context = SolrConnection.from_url("https://h", ca_file=str(tls["ca"])).ssl()
        assert isinstance(context, ssl.SSLContext)

    def test_missing_ca_file(self, tmp_path: Path) -> None:
        conn = SolrConnection.from_url("https://h", ca_file=str(tmp_path / "no.pem"))
        with pytest.raises(ValueError, match="can't use ca_file"):
            conn.ssl()

    def test_relative_ca_file(self, config_home: Path, write_toml: WriteToml) -> None:
        """Like url and path, ca_file is relative to connections.toml."""
        conns = write_toml(
            "c.toml", '[s]\nbackend = "solr"\nurl = "https://h"\nca_file = "ca.pem"\n'
        )
        with read_configfiles(store_path=conns) as cfg:
            conn = cfg.get("s", SolrConnection)
            assert conn.ca_file == str(conns.parent / "ca.pem")


@pytest.fixture(scope="session")
def tls(tmp_path_factory: pytest.TempPathFactory) -> Dict[str, Path]:
    """A throwaway CA and a server certificate for 127.0.0.1."""
    x509 = pytest.importorskip("cryptography.x509")
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    directory = tmp_path_factory.mktemp("tls")
    now = datetime.datetime.now(datetime.timezone.utc)
    pem = serialization.Encoding.PEM

    def name(common: str) -> Any:
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common)])

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = (
        x509.CertificateBuilder()
        .subject_name(name("test ca"))
        .issuer_name(name("test ca"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False
        )
        .sign(ca_key, hashes.SHA256())
    )
    # Python >= 3.13 verifies strictly (VERIFY_X509_STRICT): the server
    # certificate needs an authority key identifier, like real ones have.
    key = ec.generate_private_key(ec.SECP256R1())
    cert = (
        x509.CertificateBuilder()
        .subject_name(name("127.0.0.1"))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            False,
        )
        .add_extension(
            x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]), False
        )
        .sign(ca_key, hashes.SHA256())
    )
    paths = {"ca": directory / "ca.pem", "cert": directory / "cert.pem"}
    paths["key"] = directory / "key.pem"
    paths["ca"].write_bytes(ca.public_bytes(pem))
    paths["cert"].write_bytes(cert.public_bytes(pem))
    paths["key"].write_bytes(
        key.private_bytes(
            pem,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return paths


@pytest.fixture()
async def https_solr(
    tls: Dict[str, Path],
) -> AsyncIterator[Tuple[str, List[Dict[str, Any]]]]:
    """Like ``fake_solr``, but over HTTPS with the test CA's certificate."""
    requests: List[Dict[str, Any]] = []

    async def update(request: web.Request) -> web.Response:
        requests.append({"auth": request.headers.get("Authorization")})
        return web.json_response({"responseHeader": {"status": 0}})

    app = web.Application()
    app.router.add_post("/solr/{core}/update/json", update)
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(tls["cert"], tls["key"])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=context)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    try:
        yield f"https://127.0.0.1:{port}", requests
    finally:
        await runner.cleanup()


class TestSolrHttps:
    async def test_unknown_ca_is_rejected(
        self, https_solr: Tuple[str, List[Dict[str, Any]]], solr_plugin: None
    ) -> None:
        url, requests = https_solr
        conn = SolrConnection.from_url(url, token="t0k3n")
        with pytest.raises(aiohttp.ClientConnectorCertificateError):
            await mc_run.async_delete("solr", server=conn, facets=[("a", "b")])
        assert requests == [], "the token must not reach an unverified server"

    async def test_ca_file(
        self,
        https_solr: Tuple[str, List[Dict[str, Any]]],
        solr_plugin: None,
        tls: Dict[str, Path],
    ) -> None:
        url, requests = https_solr
        conn = SolrConnection.from_url(url, token="t0k3n", ca_file=str(tls["ca"]))
        await mc_run.async_delete("solr", server=conn, facets=[("a", "b")])
        assert [r["auth"] for r in requests] == ["Bearer t0k3n"] * 2

    async def test_without_verification(
        self, https_solr: Tuple[str, List[Dict[str, Any]]], solr_plugin: None
    ) -> None:
        url, requests = https_solr
        conn = SolrConnection.from_url(url, verify_ssl=False)
        await mc_run.async_delete("solr", server=conn, facets=[("a", "b")])
        assert len(requests) == 2
