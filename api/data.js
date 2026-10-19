// +N API: общие планы и участники. Хранилище — Upstash Redis (REST), без зависимостей.
const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

const CATS = ["quiz", "football", "cs2", "karaoke", "board", "padel", "volley", "other"];

async function redis(cmds) {
  const r = await fetch(URL_.replace(/\/$/, "") + "/pipeline", {
    method: "POST",
    headers: { Authorization: "Bearer " + TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify(cmds),
  });
  if (!r.ok) throw new Error("redis " + r.status);
  return (await r.json()).map((x) => {
    if (x.error) throw new Error(x.error);
    return x.result;
  });
}

const parseHash = (arr) => {
  const out = [];
  for (let i = 0; i < (arr || []).length; i += 2) {
    try { out.push({ id: arr[i], ...JSON.parse(arr[i + 1]) }); } catch (_) {}
  }
  return out;
};
const str = (v, max) => String(v == null ? "" : v).trim().slice(0, max);
const int = (v, lo, hi) => Math.min(hi, Math.max(lo, Math.round(Number(v) || lo)));

async function all() {
  const [a, j] = await redis([["HGETALL", "acts"], ["HGETALL", "joins"]]);
  return { acts: parseHash(a), joins: parseHash(j) };
}

function fail(res, code, error, message) {
  return res.status(code).json({ error, message });
}

module.exports = async (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  if (!URL_ || !TOKEN) return fail(res, 503, "no_storage", "Хранилище не подключено");

  try {
    if (req.method === "GET") return res.status(200).json(await all());
    if (req.method !== "POST") return fail(res, 405, "method");

    const b = typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body || {};
    const uid = str(b.uid, 64);
    if (!/^[a-z0-9]{6,64}$/i.test(uid)) return fail(res, 400, "uid", "Обнови страницу и попробуй снова");

    if (b.action === "create") {
      const a = b.act || {};
      const act = {
        title: str(a.title, 70),
        cat: CATS.includes(a.cat) ? a.cat : "other",
        startsAt: Number(a.startsAt) || 0,
        place: str(a.place, 80),
        district: str(a.district, 40),
        have: int(a.have, 1, 20),
        need: int(a.need, 1, 4),
        contact: str(a.contact, 60),
        note: str(a.note, 160),
        createdBy: uid,
        createdByName: str(b.name, 30) || "Участник",
        createdAt: Date.now(),
      };
      if (!act.title || !act.place || !act.contact || !act.startsAt)
        return fail(res, 400, "fields", "Заполни что, где, когда и контакт");
      const id = "a" + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
      await redis([["HSET", "acts", id, JSON.stringify(act)]]);
    }

    else if (b.action === "join") {
      const id = str(b.id, 64);
      const { acts, joins } = await all();
      const act = acts.find((a) => a.id === id);
      if (!act) return fail(res, 404, "gone", "Этот план уже удалён");
      if (act.createdBy === uid) return fail(res, 400, "own", "Это твой план");
      const taken = joins.filter((j) => j.act === id);
      if (!taken.some((j) => j.uid === uid)) {
        if (taken.length >= act.need) return fail(res, 409, "full", "Компания уже собралась");
        const join = { act: id, uid, name: str(b.name, 30) || "Участник", contact: str(b.contact, 40), at: Date.now(), came: null };
        await redis([["HSET", "joins", id + "__" + uid, JSON.stringify(join)]]);
      }
    }

    else if (b.action === "leave") {
      await redis([["HDEL", "joins", str(b.id, 64) + "__" + uid]]);
    }

    else if (b.action === "mark") {
      const jid = str(b.id, 140);
      const { acts, joins } = await all();
      const j = joins.find((x) => x.id === jid);
      const act = j && acts.find((a) => a.id === j.act);
      if (!act || act.createdBy !== uid) return fail(res, 403, "owner", "Отмечать может только организатор");
      const { id: _drop, ...rest } = j;
      rest.came = b.came === true ? true : b.came === false ? false : null;
      await redis([["HSET", "joins", jid, JSON.stringify(rest)]]);
    }

    else if (b.action === "delete") {
      const id = str(b.id, 64);
      const { acts, joins } = await all();
      const act = acts.find((a) => a.id === id);
      if (act && act.createdBy !== uid) return fail(res, 403, "owner", "Удалить может только организатор");
      const cmds = [["HDEL", "acts", id]];
      joins.filter((j) => j.act === id).forEach((j) => cmds.push(["HDEL", "joins", j.id]));
      await redis(cmds);
    }

    else return fail(res, 400, "action");

    return res.status(200).json(await all());
  } catch (e) {
    console.error(e);
    return fail(res, 500, "server", "Сервер не ответил. Попробуй ещё раз.");
  }
};
