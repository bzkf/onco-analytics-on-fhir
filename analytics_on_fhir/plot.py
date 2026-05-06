import matplotlib.pyplot as plt


def plot_diagnoses(output_dir, df):
    agg = (
        df.groupBy("entity_or_parent")
        .agg({"*": "count", "age_at_diagnosis": "avg"})
        .withColumnRenamed("count(1)", "n_diagnosen")
        .withColumnRenamed("avg(age_at_diagnosis)", "avg_age")
    )

    pdf = agg.toPandas()

    top20 = pdf.sort_values("n_diagnosen", ascending=False).head(20)
    plt.figure()
    plt.bar(top20["entity_or_parent"], top20["n_diagnosen"])
    plt.xticks(rotation=90)
    plt.ylabel("Anzahl Diagnosen")
    plt.title("Top 20 Entitäten nach Diagnosen")
    plt.tight_layout()
    plt.show()

    top40 = pdf.sort_values("n_diagnosen", ascending=False).head(40)
    plt.figure()
    plt.bar(top40["entity_or_parent"], top40["n_diagnosen"])
    plt.xticks(rotation=90)
    plt.ylabel("Anzahl Diagnosen")
    plt.title("Top 40 Entitäten nach Diagnosen")
    plt.tight_layout()
    plt.show()

    agg = (
        df.groupBy("entity_or_parent")
        .agg({"age_at_diagnosis": "avg"})
        .withColumnRenamed("avg(age_at_diagnosis)", "avg_age")
    )

    pdf = agg.toPandas().dropna()
    pdf = pdf.sort_values("avg_age")
    y_pos = range(len(pdf))
    plt.figure(figsize=(8, max(6, len(pdf) * 0.2)))
    plt.scatter(pdf["avg_age"], y_pos, alpha=0.6)
    plt.yticks(y_pos, pdf["entity_or_parent"])
    plt.xlabel("Durchschnittsalter")
    plt.ylabel("entity_or_parent")
    plt.title("Entitäten vs Alter")

    plt.tight_layout()
    plt.show()


# add other plotting functions here
