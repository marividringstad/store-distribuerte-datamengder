import pandas as pd
import time

df = pd.DataFrame(columns=['id', 'value'])
start_time = time.time()

# Simulate adding rows in a loop (like in your original example)
for i in range(10000):
    new_row = pd.DataFrame({'id': [i], 'value': [i * 2]})
    df = pd.concat([df, new_row], ignore_index=True)

end_time = time.time()
print(f"Time taken with DataFrame concatenation: {end_time - start_time:.5f} seconds")
