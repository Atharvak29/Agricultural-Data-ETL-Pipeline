from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from time import sleep

driver = webdriver.Chrome()
driver.get("https://agmarknet.gov.in/home")

# Initial load pause
sleep(5) 
wait = WebDriverWait(driver, 15)

# -----------------------------------------
# Helper: Select dropdown item with human pauses
# -----------------------------------------
def select_from_dropdown(element_id, search_text, uncheck_label):
    # Open dropdown
    driver.find_element(By.ID, element_id).click()
    sleep(1)  # Pause for dropdown animation

    # Uncheck "All..." option
    wait.until(EC.element_to_be_clickable(
        (By.XPATH, f"//span[text()='{uncheck_label}']")
    )).click()
    sleep(0.5) # Small pause after unchecking

    # Type in the search bar
    search_input = wait.until(EC.visibility_of_element_located(
        (By.XPATH, "//input[@placeholder='Search...']")
    ))
    search_input.clear()
    search_input.send_keys(search_text)
    sleep(1) # Pause to simulate typing speed

    # Click the matching item
    wait.until(EC.element_to_be_clickable(
        (By.XPATH, f"//span[contains(text(),'{search_text}')]")
    )).click()
    sleep(1) # Pause to let the selection register


### ---- 1️⃣ Select State ---- ###
print("Selecting State...")
select_from_dropdown("state", "Maharashtra", "All States")


### ---- 2️⃣ Select District ---- ###
print("Selecting District...")
select_from_dropdown("district", "Pune", "All Districts")


### ---- 3️⃣ Select Commodity ---- ###
print("Selecting Commodity...")
# specific string provided by you
select_from_dropdown("commodity", "Bajra(Pearl Millet/Cumbu)", "All Commodities")


### ---- 4️⃣ Click GO button ---- ###
print("Clicking Go...")
go_button = wait.until(EC.element_to_be_clickable(
    (By.XPATH, "//button[span[contains(text(),'Go')]]")
))

# Use JS Click to avoid "ElementClickInterceptedException"
driver.execute_script("arguments[0].click();", go_button)

# Pause to allow the table/no-data message to load
sleep(3) 


### ---- 5️⃣ Check if No Data Available is present ---- ###
no_data_xpath = "//h3[contains(text(),'No Data Available')]"

if len(driver.find_elements(By.XPATH, no_data_xpath)) > 0:
    print("\n❌ Data Not Available for these filters.")
    driver.quit()
    exit() 


### ---- 6️⃣ Wait for and Extract Data ---- ###
try:
    table = wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
    
    # Get the latest row (first row in tbody)
    latest_row = table.find_elements(By.XPATH, ".//tbody/tr")[0]

    # Extract columns
    columns = latest_row.find_elements(By.TAG_NAME, "td")

    # Safety check: ensure the row has enough columns before accessing index
    if len(columns) >= 7:
        # market    = columns[0].text
        # variety   = columns[1].text
        # grade     = columns[2].text
        latest_price = columns[3].text
        # max_price = columns[4].text
        # modal     = columns[5].text
        arrival   = columns[6].text

        print("\n📌 Latest Data")
        print("------------------------")
        
        print(f"Modal_Price_Rs_Qtl   : {latest_price}")
        print(f"Arrival_Quantity_Qtl : {arrival}")
        print("------------------------\n")
    else:
        print("⚠️ Table row found, but it looks empty or has unexpected format.")

except Exception as e:
    print(f"\n⚠️ Error extracting data: {e}")

sleep(2)
driver.quit()