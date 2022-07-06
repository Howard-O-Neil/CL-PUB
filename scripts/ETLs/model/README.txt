re1 = content only
re2 = content + org_rank
re3 = content + org_discrete

best_org = max(org_rank, org_discrete)

re4 = content + best_org + rwr_bias
re5 = content + best_org + rwr_bias + active

re6 = best_org
re7 = best_org + rwr_bias
re8 = best_org + rwr_bias + active

best_re = max(re1, re2, re3, re4, re5, re6, re7, re8)

deep_re1 = 