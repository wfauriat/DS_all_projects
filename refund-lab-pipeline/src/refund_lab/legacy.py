

# with open("orders_sample.jsonl", "w") as f:
#     for order in orders:
#         f.write(json.dumps(order) + "\n")

# def make_flaky_fetch_page(real_fetch_page, fail_pattern):
#     """
#     fail_pattern: a list of exceptions (or None for success) to return in order,
#     one per call. Once exhausted, falls back to real_fetch_page.
#     e.g. fail_pattern = [RetryableBackoff(), RetryableBackoff(), None]
#     forces two backoff failures then a real call on the third.
#     """
#     pattern = iter(fail_pattern)

#     def flaky_fetch_page(client, cursor):
#         try:
#             outcome = next(pattern)
#         except StopIteration:
#             return real_fetch_page(client, cursor)
#         if outcome is None:
#             return real_fetch_page(client, cursor)
#         raise outcome

#     return flaky_fetch_page

# fetch_page = make_flaky_fetch_page(
#     fetch_page,
#     fail_pattern=[
#         RetryableBackoff(retry_after=1.5),   # should sleep exactly 1.5s
#         RetryableBackoff(retry_after=None),  # should compute exponential+jitter
#         None,
#     ]
# )