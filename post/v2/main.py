
# =========================
# MAIN
# =========================
import urllib, time, sys
from datetime import datetime
from configs import *
from automation import bootstrap_auth,  install_early_hook, start_driver_with_proxy, wait_next_req
from checkpoint import load_checkpoint, normalize_seen_ids, save_checkpoint
from get_posts_fb_automation import paginate_window, run_cursor_only
from utils import   get_vars_from_form, is_group_feed_req, make_vars_template, parse_form, update_vars_for_next_cursor
# from get_posts_fb_automation import start_driver
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--resume", action="store_true",
                    help="Tiếp tục từ cursor trong checkpoint thay vì bám head.")
    ap.add_argument("--page-limit", type=int, default=None,
                    help="Giới hạn số trang để test (None = không giới hạn).")
    ap.add_argument("--backfill", action="store_true",
                    help="Crawl ngược thời gian (ví dụ từ tháng 8/2015 đến tháng 6/2015).")
    ap.add_argument("--from-month", type=int, default=None, help="Tháng bắt đầu (ví dụ: 8).")
    ap.add_argument("--to-month", type=int, default=None, help="Tháng kết thúc (ví dụ: 6).")
    ap.add_argument("--year", type=int, default=None, help="Năm (ví dụ: 2015).")

    args = ap.parse_args()

    d = start_driver_with_proxy(PROXY_URL, headless=False)
    d.set_script_timeout(40)
    try:
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception:
        pass

    bootstrap_auth(d)
    try:
        install_early_hook(d, keep_last=KEEP_LAST)
    except Exception as e:
        print("[WARN] install_early_hook:", e)

    d.get(GROUP_URL); time.sleep(1.2)
    for _ in range(6):
        d.execute_script("window.scrollBy(0, Math.floor(window.innerHeight*0.9));"); time.sleep(0.6)

    nxt = wait_next_req(d, 0, is_group_feed_req, timeout=25, poll=0.25)
    if not nxt:
        raise RuntimeError("Không bắt được request feed. Hãy cuộn thêm/kiểm tra quyền.")
    _, first_req = nxt
    form         = parse_form(first_req.get("body", ""))
    friendly     = urllib.parse.parse_qs(first_req.get("body","")).get("fb_api_req_friendly_name", [""])[0]
    vars_now     = get_vars_from_form(form)
    template_now = make_vars_template(vars_now)

    state = load_checkpoint()
    seen_ids      = normalize_seen_ids(state.get("seen_ids"))
    cursor_ckpt   = state.get("cursor")
    vars_template = state.get("vars_template") or template_now
    effective_template = vars_template or template_now
    if args.backfill and args.year and args.from_month and args.to_month:
        print(f"[MODE] Backfill từ tháng {args.from_month}/{args.year} → {args.to_month}/{args.year}")
        cur = args.from_month
        while cur >= args.to_month:
            start = datetime.datetime(args.year, cur, 1)
            if cur == 1:
                end = datetime.datetime(args.year - 1, 12, 1)
            else:
                end = datetime.datetime(args.year, cur - 1, 1)

            t_from = int(end.timestamp())
            t_to = int(start.timestamp())

            print(f"\n🕰️ Crawling trước {start.strftime('%Y-%m-%d')} ...")
            total_new, min_created, has_next = paginate_window(
                d, form, effective_template, seen_ids=set(),
                t_from=t_from,
                t_to=t_to,
                page_limit=args.page_limit
            )
            print(f"✅ Done {start.strftime('%Y-%m')} → {total_new} posts | min_created={min_created}")
            save_checkpoint(cursor=None, seen_ids=list(seen_ids),
                            vars_template=effective_template,
                            mode="time", slice_from=None, slice_to=t_to, year=args.year)
            time.sleep(2)
            cur -= 1

        print("\n🎉 [DONE] Backfill completed.")
        d.quit()
        sys.exit(0)

    # ✅ Resume đúng vị trí (nếu có --resume và có cursor trong checkpoint)
    if args.resume and cursor_ckpt:
        form = update_vars_for_next_cursor(form, cursor_ckpt, vars_template=effective_template)
        print(f"[RESUME] Dùng lại cursor từ checkpoint: {str(cursor_ckpt)[:40]}...")

    # 🔁 Chạy crawl theo cursor-only (không time-slice)
    total_got = run_cursor_only(
        d, form, effective_template, seen_ids,
        page_limit=args.page_limit,
        resume=args.resume   # ✅ quan trọng
    )

    # Lưu checkpoint cuối (giữ seen_ids & template; cursor đã được cập nhật trong quá trình paginate)
    save_checkpoint(cursor=None, seen_ids=list(seen_ids), vars_template=effective_template,
                    mode=None, slice_from=None, slice_to=None, year=None)
    print(f"[DONE] total new written (cursor-only) = {total_got} → {OUT_NDJSON}")
