package io.lmb.gateway;

import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
import io.lmb.proto.orderentry.ListSymbolsRequest;
import io.lmb.proto.orderentry.OrderQueryGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** gRPC over TLS and mutual TLS, with throwaway certificates from openssl. */
class GatewayTlsTest
{
    @TempDir
    Path dir;

    private final Fixture fx = new Fixture();

    @AfterEach
    void tearDown()
    {
        fx.close();
    }

    private void openssl(final String... args) throws Exception
    {
        final Process p = new ProcessBuilder(List.of(args)).directory(dir.toFile()).redirectErrorStream(true).start();
        final String out = new String(p.getInputStream().readAllBytes());
        if (!p.waitFor(30, TimeUnit.SECONDS) || p.exitValue() != 0)
        {
            throw new IllegalStateException(String.join(" ", args) + " failed: " + out);
        }
    }

    /** A CA, a server certificate for localhost / 127.0.0.1 and a client certificate, all EC P-256. */
    private void certificates() throws Exception
    {
        Assumptions.assumeTrue(new File("/usr/bin/openssl").canExecute() || new File("/usr/local/bin/openssl").canExecute(),
            "openssl not installed");
        Files.writeString(dir.resolve("server.ext"), "subjectAltName=DNS:localhost,IP:127.0.0.1\n");
        Files.writeString(dir.resolve("client.ext"), "extendedKeyUsage=clientAuth\n");
        openssl("openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
            "-keyout", "ca.key", "-out", "ca.pem", "-days", "2", "-subj", "/CN=lmb-test-ca");
        for (final String who : List.of("server", "client"))
        {
            openssl("openssl", "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                "-keyout", who + ".key", "-out", who + ".csr", "-subj", "/CN=" + (who.equals("server") ? "localhost" : "team-alpha"));
            openssl("openssl", "x509", "-req", "-in", who + ".csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
                "-out", who + ".pem", "-days", "2", "-extfile", who + ".ext");
        }
    }

    private GatewayConfig tlsConfig()
    {
        final GatewayConfig cfg = Fixture.config(1);
        cfg.tlsCertChain = dir.resolve("server.pem").toString();
        cfg.tlsPrivateKey = dir.resolve("server.key").toString();
        return cfg;
    }

    private int symbols(final int port, final ChannelCredentials creds) throws Exception
    {
        final ManagedChannel ch = Grpc.newChannelBuilderForAddress("localhost", port, creds).build();
        try
        {
            return OrderQueryGrpc.newBlockingStub(ch).withDeadlineAfter(5, TimeUnit.SECONDS)
                .listSymbols(ListSymbolsRequest.getDefaultInstance()).getSymbolsCount();
        }
        finally
        {
            ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void serverTls() throws Exception
    {
        certificates();
        final Gateway g = fx.gateway(tlsConfig());
        final File ca = dir.resolve("ca.pem").toFile();
        assertEquals(3, symbols(g.grpcPort(), TlsChannelCredentials.newBuilder().trustManager(ca).build()));
        assertThrows(StatusRuntimeException.class, () -> symbols(g.grpcPort(), InsecureChannelCredentials.create()),
            "plaintext refused");
        assertThrows(StatusRuntimeException.class, () -> symbols(g.grpcPort(), TlsChannelCredentials.create()),
            "a client that does not trust the venue's CA");
    }

    @Test
    void mutualTls() throws Exception
    {
        certificates();
        final GatewayConfig cfg = tlsConfig();
        cfg.tlsClientCa = dir.resolve("ca.pem").toString();
        final Gateway g = fx.gateway(cfg);
        final File ca = dir.resolve("ca.pem").toFile();
        assertThrows(StatusRuntimeException.class,
            () -> symbols(g.grpcPort(), TlsChannelCredentials.newBuilder().trustManager(ca).build()), "no client certificate");
        assertEquals(3, symbols(g.grpcPort(), TlsChannelCredentials.newBuilder().trustManager(ca)
            .keyManager(dir.resolve("client.pem").toFile(), dir.resolve("client.key").toFile()).build()));
    }
}
