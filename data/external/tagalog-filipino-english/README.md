---
license: apache-2.0
task_categories:
- translation
language:
- en
- tl
size_categories:
- 10K<n<100K

dataset_info:
  features:
    - name: tagalog
      dtype: string
    - name: english
      dtype: string
  splits:
    - name: train
      num_bytes: 71827456
      num_examples: 84177
    - name: test
      num_bytes: 18035507.2
      num_examples: 21057
---

This dataset is a Tagalog-English translation data. It is a compiled comma-separated values dataset from different 
existing HuggingFace and External dataset.

Here are the collected and compiled data:
1. saillab/alpaca_tamil_taco
2. DIBT/MPEP_FILIPINO
3. Nag, S., Ma, S., Ntalli, A., & Dulay, K. M. (2024, June 10). TalkTogether. https://doi.org/10.17605/OSF.IO/3ZDFN