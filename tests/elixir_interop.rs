//! End-to-end test against a real Elixir node. Skipped (with a note) when
//! `elixir` is not on PATH. Needs EPMD, which the node starts if missing.

use rex::{Dest, Error, Exception, FnModule, Node, Signal, SpawnOpts, Term};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;

fn hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "localhost".into())
}

fn elixir_available() -> bool {
    std::process::Command::new("elixir")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

#[tokio::test]
async fn round_trip_with_elixir() {
    if !elixir_available() {
        eprintln!("skipping: elixir not found on PATH");
        return;
    }
    let host = hostname();
    let tag = std::process::id();
    let rex_name = format!("rex_test_{tag}@{host}");
    let ex_name = format!("ex_test_{tag}@{host}");
    let cookie = format!("rexcookie{tag}");

    let node = Node::builder(&rex_name)
        .cookie(&cookie)
        .module(
            FnModule::new("test")
                .function("add", 2, |args, _| {
                    let a = args[0].as_int().ok_or_else(Exception::badarg)?;
                    let b = args[1].as_int().ok_or_else(Exception::badarg)?;
                    Ok(Term::Int(a + b))
                })
                .function("echo", 1, |mut args, _| Ok(args.pop().unwrap())),
        )
        .start()
        .await
        .expect("node starts (is epmd installed?)");
    let mut inbox = node.spawn_mailbox();
    inbox.register("inbox").unwrap();

    let script = format!(
        r#"
        rex = :"{rex_name}"
        true = Node.connect(rex)
        3 = :erpc.call(rex, :test, :add, [1, 2])
        {{:badrpc, {{:EXIT, {{:undef, _}}}}}} = :rpc.call(rex, :nope, :nope, [])
        Process.register(self(), :test_server)
        send({{:inbox, rex}}, {{self(), :ready, node()}})
        # 3 MB message: fragmented by the BEAM now that we negotiate FRAGMENTS.
        big = :binary.copy(<<7>>, 3_000_000)
        send({{:inbox, rex}}, {{self(), :big, big}})
        receive do
          {{:big_back, ^big}} -> :ok
        after 10_000 -> IO.puts("ELIXIR_NO_BIG")
        end
        ^big = :erpc.call(rex, :test, :echo, [big])
        receive do
          {{:"$gen_call", from, :hi}} -> GenServer.reply(from, :hello)
        after 10_000 -> IO.puts("ELIXIR_NO_CALL")
        end
        receive do
          :done -> IO.puts("ELIXIR_OK")
        after 10_000 -> IO.puts("ELIXIR_TIMEOUT")
        end
        "#
    );
    let mut child = tokio::process::Command::new("elixir")
        .args(["--sname", &ex_name, "--cookie", &cookie, "-e", &script])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Elixir connects, calls us, then announces itself.
    let msg = tokio::time::timeout(Duration::from_secs(60), inbox.recv_message())
        .await
        .expect("elixir never reported in")
        .unwrap();
    let items = msg.as_tuple().unwrap();
    let ex_pid = items[0].as_pid().unwrap().clone();
    assert!(items[1].is_atom("ready"));
    let ex_node = items[2].as_atom().unwrap().clone();
    assert_eq!(ex_node.as_str(), ex_name);
    assert!(node.is_connected(&ex_node));

    // A fragmented 3 MB message arrives whole; echo it back.
    let msg = tokio::time::timeout(Duration::from_secs(60), inbox.recv_message())
        .await
        .expect("big message never arrived")
        .unwrap();
    let items = msg.as_tuple().unwrap();
    assert!(items[1].is_atom("big"));
    assert_eq!(items[2].as_binary().map(|b| b.len()), Some(3_000_000));
    node.send(&ex_pid, Term::tuple(vec![Term::atom("big_back"), items[2].clone()]))
        .await
        .unwrap();

    // Rust -> Elixir: GenServer-style call and classic rpc.
    let reply = node
        .call(
            Dest::named("test_server", ex_node.clone()),
            Term::atom("hi"),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(reply.is_atom("hello"));
    let n = node
        .rpc_call(
            ex_node.as_str(),
            "erlang",
            "node",
            vec![],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(n.as_atom().unwrap(), &ex_node);
    let sum = node
        .rpc_call(
            ex_node.as_str(),
            "lists",
            "sum",
            vec![Term::list(vec![1.into(), 2.into(), 3.into()])],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(sum, Term::Int(6));

    // erpc-style calls through spawn_request.
    let n = node
        .erpc_call(ex_node.as_str(), "erlang", "node", vec![], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(n.as_atom().unwrap(), &ex_node);
    let sum = node
        .erpc_call(
            ex_node.as_str(),
            "lists",
            "sum",
            vec![Term::list(vec![4.into(), 5.into()])],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(sum, Term::Int(9));
    match node
        .erpc_call(ex_node.as_str(), "erlang", "error", vec![Term::atom("boom")], Duration::from_secs(5))
        .await
    {
        Err(Error::Exception(Exception::Error(t))) => assert!(t.is_atom("boom")),
        other => panic!("expected error exception, got {other:?}"),
    }
    match node
        .erpc_call(ex_node.as_str(), "erlang", "throw", vec![Term::atom("ball")], Duration::from_secs(5))
        .await
    {
        Err(Error::Exception(Exception::Throw(t))) => assert!(t.is_atom("ball")),
        other => panic!("expected throw, got {other:?}"),
    }
    match node
        .erpc_call(ex_node.as_str(), "nope", "nope", vec![], Duration::from_secs(5))
        .await
    {
        Err(Error::Exception(Exception::Error(t))) => assert!(t.is_atom("undef")),
        other => panic!("expected undef, got {other:?}"),
    }
    // Raw spawn_request with a monitor: the reply, then the DOWN.
    let mut mb = node.spawn_mailbox();
    let req = node
        .spawn_request(
            &mb,
            ex_node.as_str(),
            "erlang",
            "exit",
            vec![Term::atom("bye")],
            SpawnOpts { monitor: true, link: false },
        )
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(5), mb.recv_message()).await.unwrap().unwrap();
    let r = reply.as_tuple().unwrap();
    assert!(r[0].is_atom("spawn_reply") && r[1].as_reference() == Some(&req) && r[2].is_atom("ok"), "{reply}");
    assert_eq!(r[3].as_pid().map(|p| &p.node), Some(&ex_node));
    match tokio::time::timeout(Duration::from_secs(5), mb.recv()).await.unwrap().unwrap() {
        Signal::Down { reference, reason, .. } => {
            assert_eq!(reference, req);
            assert!(reason.is_atom("bye"), "{reason}");
        }
        other => panic!("expected DOWN, got {other:?}"),
    }

    node.send(&ex_pid, Term::atom("done")).await.unwrap();

    let mut out = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut out)
        .await
        .unwrap();
    let mut err = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut err)
        .await
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(
        status.success(),
        "elixir exited with {status}\nstdout: {out}\nstderr: {err}"
    );
    assert!(out.contains("ELIXIR_OK"), "stdout: {out}\nstderr: {err}");

    // The Elixir node going away must surface as a node down event.
    let mut events = node.events();
    let down = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(rex::NodeEvent::NodeDown(n)) = events.recv().await {
                if n == ex_node {
                    break;
                }
            }
        }
    })
    .await;
    assert!(down.is_ok() || !node.is_connected(&ex_node));
    drop(inbox);
    node.shutdown();
}
