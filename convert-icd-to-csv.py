import csv
import sys
import os

def convert_fixed_width_to_csv(input_filename, output_filename, positions):
    """
    Generic converter for Medicare fixed-width text files.
    """
    if not os.path.exists(input_filename):
        print(f"Error: Source file '{input_filename}' not found.")
        return

    print(f"Starting conversion: {input_filename} -> {output_filename}...")
    
    try:
        with open(input_filename, 'r', encoding='utf-8') as txtfile, \
             open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            
            writer = csv.writer(csvfile)
            # Write professional headers
            writer.writerow([p[2] for p in positions])
            
            count = 0
            for line in txtfile:
                if not line.strip():
                    continue
                
                row = []
                for start, end, name in positions:
                    # Slice the string. If end is None, it reads to the end of the line.
                    value = line[start:end].strip() if end else line[start:].strip()
                    row.append(value)
                writer.writerow(row)
                count += 1
        
        print(f"Successfully converted {count} rows to {output_filename}")
    except Exception as e:
        print(f"An error occurred: {e}")

# --- CONFIGURATIONS ---

# 1. ICD-10 CM Order File (The one you have)
ICD_CONFIG = [
    (0, 5, 'sort_order'),
    (6, 13, 'icd10_code'),
    (14, 15, 'is_billable'),
    (16, 76, 'description_short'),
    (77, None, 'description_long')
]

# 2. HCPCS Level II (Standard CMS Layout)
HCPCS_CONFIG = [
    (0, 5, 'hcpcs_code'),
    (5, 50, 'short_description'),
    (50, None, 'long_description')
]

# 3. CPT / Physician Fee Schedule (Basic Layout)
CPT_CONFIG = [
    (0, 5, 'cpt_code'),
    (13, 41, 'short_description'),
    (41, None, 'long_description')
]

# --- EXECUTION LOGIC ---

if __name__ == "__main__":
    # Convert ICD-10
    convert_fixed_width_to_csv('medicare-dsd-evidence/data/icd-codes/icd10cm_order_2026.txt', 'medicare-dsd-evidence/data/icd-codes/ref_icd10_diagnosis.csv', ICD_CONFIG)
     
    # Convert CPT (Uncomment and update filename when you download it)
    # convert_fixed_width_to_csv('cpt_source.txt', 'ref_cpt_procedures.csv', CPT_CONFIG)