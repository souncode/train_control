import hmac

from flask import abort, jsonify, redirect, render_template, request, session, url_for


def register_basic_routes(
    app,
    *,
    get_next_url,
    is_authenticated,
    load_user_credentials,
    verify_password,
    mark_authenticated,
    clear_auth,
    state_lock,
    state,
    monitor_cache,
    now_str,
    root_dir,
    train_monitor_host,
    train_monitor_port,
    resolve_project_path,
):
    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = (request.form.get("username", "") or "").strip()
            password = request.form.get("password", "") or ""
            next_url = get_next_url(default="/")
            creds = load_user_credentials()
            expected_username = str(creds.get("username", "")).strip()

            if hmac.compare_digest(username, expected_username) and verify_password(password, creds):
                mark_authenticated(expected_username)
                return redirect(next_url)

            return render_template(
                "login.html",
                error="Invalid username or password",
                next_url=next_url
            ), 401

        if is_authenticated():
            return redirect(get_next_url(default="/"))

        return render_template(
            "login.html",
            error="",
            next_url=get_next_url(default="/")
        )

    @app.route("/logout", methods=["POST"])
    def logout():
        clear_auth()
        return jsonify({"ok": True, "message": "Logged out"})

    @app.route("/healthz")
    def healthz():
        with state_lock:
            payload = {
                "ok": True,
                "worker_running": bool(state.get("worker_running")),
                "queue_size": len(state.get("queue") or []),
                "project_count": len(state.get("projects") or {}),
                "version": int(state.get("version", 0)),
                "monitor_ok": bool(monitor_cache.get("status_ok") or monitor_cache.get("history_ok")),
                "time": now_str(),
            }
        return jsonify(payload)

    @app.route("/")
    def index():
        creds = load_user_credentials()
        default_user = str(creds.get("username", "")).strip() or "admin"
        return render_template(
            "index.html",
            root_dir=str(root_dir),
            train_monitor_url=f"http://{train_monitor_host}:{train_monitor_port}",
            auth_user=session.get("auth_user", default_user)
        )

    @app.route("/project_editor")
    def project_editor_page():
        project = request.args.get("project", "").strip()
        if not project:
            return redirect(url_for("index"))

        project_path = resolve_project_path(project)
        if not project_path or not project_path.exists():
            abort(404)

        return render_template("project_editor.html", project_name=project)
