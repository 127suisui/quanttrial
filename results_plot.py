import json, sys, matplotlib.pyplot as plt

data = json.load(open(sys.argv[1]))
labels = list(data["baselines"].keys()) + ["optimized"]
values = [b["total_cash"] for b in data["baselines"].values()] + [data["optimized"]["total_cash"]]

plt.bar(labels, values)
plt.ylabel("Total cash spent")
plt.title("Execution cost comparison")
plt.tight_layout()
plt.savefig("results.png")
print("saved results.png")
