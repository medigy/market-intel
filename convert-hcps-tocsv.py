import csv

def convert_hcpcs_to_csv(input_filename, output_filename):
    # These are the EXACT character positions from HCPC2026_recordlayout.txt
    HCPCS_LAYOUT = [
        (0, 5, 'hcpcs_code'),
        (5, 10, 'seq_num'),
        (10, 11, 'record_id'),
        (11, 91, 'long_description'),
        (91, 119, 'short_description')
    ]

    with open(input_filename, 'r', encoding='utf-8') as txtfile, \
         open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        
        writer = csv.writer(csvfile)
        writer.writerow([p[2] for p in HCPCS_LAYOUT])
        
        for line in txtfile:
            # Skip empty lines
            if not line.strip(): continue
            
            # Extract RIC (Record ID) at position 10
            record_id = line[10:11]
            
            # ONLY keep RIC '3' (Master Procedure Records) 
            # and RIC '7' (Master Modifier Records)
            if record_id in ('3', '7'):
                row = []
                for start, end, name in HCPCS_LAYOUT:
                    val = line[start:end].strip()
                    row.append(val)
                writer.writerow(row)

    print(f"Professional HCPCS conversion complete: {output_filename}")

# Run the fixed conversion
convert_hcpcs_to_csv('medicare-dsd-evidence/data/hcps-codes/HCPC2026_APR_ANWEB.txt', 'medicare-dsd-evidence/data/hcps-codes/ref_hcpcs_level_two_procedures.csv')