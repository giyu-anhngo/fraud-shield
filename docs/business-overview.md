# FraudShield — Business Overview

*A plain-language description of what FraudShield is, what it does, and why. Use this as the foundation for writing requirements and setting up the codebase.*

---

## 1. What is FraudShield?

FraudShield watches payment transactions and finds the ones that look **suspicious or unauthorized** — payments that were probably not made by the real account owner.

You have likely seen this in real life: you buy something, and a minute later your bank sends a message — *"Did you make this purchase?"* FraudShield is a small version of the system behind that message.

In one sentence: **FraudShield is a near-real-time fraud monitoring system that flags suspicious payments for review and alerts the customer.**

---

## 2. The problem it solves

Most payments are normal. A very small number are fraud. The job is to separate the suspicious few from the normal many — quickly and accurately.

Being wrong is costly in two different ways:

- **Miss a fraud** → the business loses money and trust.
- **Wrongly flag a real payment** → a real customer is annoyed and a real sale may be lost.

So the system is always balancing these two mistakes. It should catch as much fraud as possible **without** blocking too many honest customers. This balance is the single most important idea in the whole project.

> Important wording: a fraudulent payment is usually a *valid* payment (real card, real money). What is wrong is that it is **unauthorized** or **unusual for that account** — not "invalid".

---

## 3. Who uses the system (Actors)

| Actor | Role |
|-------|------|
| **Customer / cardholder** | The person whose money it is. Receives alerts ("Did you make this purchase?"). |
| **Transaction source** | The upstream system that sends payments into FraudShield. |
| **Fraud analyst** | A person who investigates flagged payments and decides if they are really fraud. |
| **The system itself** | Records, scores, flags, and notifies automatically. |

---

## 4. What the system does (in scope)

FraudShield can:

1. **Record every payment** as a permanent, trusted record.
2. **Check each payment against fraud rules** and give it a risk decision: *normal* or *suspicious*.
3. **Create a case** for each suspicious payment, so an analyst can investigate it.
4. **Alert the customer and/or analyst** when a payment is flagged.
5. **Let analysts search and review** past cases and mark each one as real fraud or a false alarm.

---

## 5. What the system does NOT do (out of scope)

Saying this clearly keeps the project focused:

- It **does not block** a payment before it is approved. It monitors payments **after** they are accepted (post-authorization).
- It **does not use machine learning**. Detection is based on **simple, clear rules**.
- It **does not process or settle payments** itself. It only watches them.

---

## 6. Key business concepts

These are the main "things" in the system. They will become your data model.

- **Transaction** — a single payment. Includes: account, amount, currency, time, merchant, location/country, and channel (in-store or online).
- **Account** — the customer's account. Includes: home country, status, and the account's **normal behavior** (typical amount, usual locations). This is what lets the system say "this is unusual *for this account*".
- **Risk score** — a number that says how suspicious a payment looks.
- **Fraud decision** — the result: *normal* or *suspicious*, plus which rules were triggered.
- **Fraud case** — a suspicious payment that an analyst needs to review.
- **False positive** — a normal payment that was wrongly flagged.
- **False negative** — a fraud that the system missed.
- **Chargeback** — the money reversal that happens when fraud is confirmed. This is the real-world cost of a missed fraud.

---

## 7. What makes a payment suspicious (the rules)

These are the checks the system runs. Each one maps to a real fraud pattern.

| Rule | What it checks | Why it matters |
|------|----------------|----------------|
| **Large amount** | Amount is far higher than this account's normal spending | A stolen card is often used to buy something expensive fast |
| **Velocity** | Too many payments in a short time | Cards are often tested or drained quickly |
| **Impossible travel** | Two payments in distant places too close in time | One card cannot be in two countries at once |
| **Country mismatch** | Payment country is different from the account's home country | Catches cross-border misuse |
| **Blacklist** | Merchant, device, or IP is already known to be bad | Catches repeat offenders |
| **Dormant then active** | A quiet account suddenly makes a big payment | A sign of account takeover |

> Keep these rules **simple**. The value of the project is the system design, not a clever detection algorithm.

---

## 8. The life of a payment (lifecycle)

Step by step, what happens to one payment:

1. A payment **arrives** and is **recorded**.
2. The payment is **scored** against the rules.
3. If it looks **normal**, it passes silently.
4. If it looks **suspicious**, a **case is created** and an **alert is sent**.
5. An **analyst reviews** the case.
6. The analyst **resolves** it: confirmed fraud, or false alarm.

---

## 9. The fraud case lifecycle (states)

A case moves through clear states:

```
OPEN  ->  UNDER_REVIEW  ->  CONFIRMED_FRAUD
                        ->  CLEARED (false alarm)
```

- **OPEN** — newly created, waiting for an analyst.
- **UNDER_REVIEW** — an analyst is investigating.
- **CONFIRMED_FRAUD** — it was real fraud.
- **CLEARED** — it was a normal payment (a false positive).

---

## 10. The one idea that guides every decision

Catch as much fraud as possible, **without** blocking too many honest customers.

- Stricter rules → catch more fraud, but flag more good payments (more false positives).
- Looser rules → annoy fewer customers, but miss more fraud (more false negatives).

There is no setting that wins on both sides. Every requirement, rule, and threshold in this project is a choice about where to sit on this balance.

---

*Next step: turn each capability in Section 4 into a functional requirement, each concept in Section 6 into a data entity, and each rule in Section 7 into a testable rule with a clear threshold.*