from dagger import dsl


@dsl.task()
def find_user_cohorts():
    return ["baby-boomers", "millenials", "gen-z"]


@dsl.task()
def find_product_categories():
    return ["veggies", "meat"]


@dsl.task()
def generate_recommendations(user_cohort, product_category):
    pass


@dsl.DAG()
def generate_recommendations_for_cohort(user_cohort):
    for product_category in find_product_categories():
        generate_recommendations(user_cohort, product_category)


@dsl.DAG()
def dag():
    for user_cohort in find_user_cohorts():
        generate_recommendations_for_cohort(user_cohort)
