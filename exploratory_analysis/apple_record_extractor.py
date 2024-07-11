import pandas as pd

from constants import Apple, FileDirectory
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()

RECORD_TYPE = [
    "HKQuantityTypeIdentifierBloodGlucose"
    "HKQuantityTypeIdentifierDietaryWater"
    "HKQuantityTypeIdentifierBodyMassIndex"
    "HKQuantityTypeIdentifierBodyMass"
    "HKQuantityTypeIdentifierHeartRate"
    "HKQuantityTypeIdentifierOxygenSaturation"
    "HKQuantityTypeIdentifierRespiratoryRate"
    "HKQuantityTypeIdentifierStepCount"
    "HKQuantityTypeIdentifierDistanceWalkingRunning"
    "HKQuantityTypeIdentifierBasalEnergyBurned"
    "HKQuantityTypeIdentifierActiveEnergyBurned"
    "HKQuantityTypeIdentifierFlightsClimbed"
    "HKQuantityTypeIdentifierDietaryFatTotal"
    "HKQuantityTypeIdentifierDietaryFatSaturated"
    "HKQuantityTypeIdentifierDietaryCholesterol"
    "HKQuantityTypeIdentifierDietarySodium"
    "HKQuantityTypeIdentifierDietaryCarbohydrates"
    "HKQuantityTypeIdentifierDietaryFiber"
    "HKQuantityTypeIdentifierDietarySugar"
    "HKQuantityTypeIdentifierDietaryEnergyConsumed"
    "HKQuantityTypeIdentifierDietaryProtein"
    "HKQuantityTypeIdentifierDietaryPotassium"
    "HKQuantityTypeIdentifierAppleExerciseTime"
    "HKQuantityTypeIdentifierRestingHeartRate"
    "HKQuantityTypeIdentifierVO2Max"
    "HKQuantityTypeIdentifierWalkingHeartRateAverage"
    "HKQuantityTypeIdentifierEnvironmentalAudioExposure"
    "HKQuantityTypeIdentifierHeadphoneAudioExposure"
    "HKQuantityTypeIdentifierWalkingDoubleSupportPercentage"
    "HKQuantityTypeIdentifierSixMinuteWalkTestDistance"
    "HKQuantityTypeIdentifierAppleStandTime"
    "HKQuantityTypeIdentifierWalkingSpeed"
    "HKQuantityTypeIdentifierWalkingStepLength"
    "HKQuantityTypeIdentifierWalkingAsymmetryPercentage"
    "HKQuantityTypeIdentifierStairAscentSpeed"
    "HKQuantityTypeIdentifierStairDescentSpeed"
    "HKDataTypeSleepDurationGoal"
    "HKQuantityTypeIdentifierAppleWalkingSteadiness"
    "HKQuantityTypeIdentifierRunningStrideLength"
    "HKQuantityTypeIdentifierRunningVerticalOscillation"
    "HKQuantityTypeIdentifierRunningGroundContactTime"
    "HKQuantityTypeIdentifierHeartRateRecoveryOneMinute"
    "HKQuantityTypeIdentifierRunningPower"
    "HKQuantityTypeIdentifierRunningSpeed"
    "HKQuantityTypeIdentifierTimeInDaylight"
    "HKQuantityTypeIdentifierPhysicalEffort"
    "HKCategoryTypeIdentifierSleepAnalysis"
    "HKCategoryTypeIdentifierAppleStandHour"
    "HKCategoryTypeIdentifierMindfulSession"
    "HKCategoryTypeIdentifierLowHeartRateEvent"
    "HKCategoryTypeIdentifierAudioExposureEvent"
    "HKCategoryTypeIdentifierHandwashingEvent"
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN"
]


def extract_sleep_data(root, record_type):
    logger.info("Extracting sleep data from Apple Health XML...")

    try:
        apple_records = []
        for record in root.findall(".//Record"):
            if record.get("type") == record_type:
                apple_record = record.attrib
                apple_records.append(apple_record)

        df = pd.DataFrame(apple_records)

        logger.info(
            f"Successfully extracted {len(df)} sleep records with {len(df.columns)} fields."
        )
        return df

    except Exception as e:
        logger.error(f"Error extracting {record_type} data: {str(e)}")
        raise


def main():
    record_type = "HKCategoryTypeIdentifierMindfulSession"

    logger.info("Starting Apple {record_type} data extraction...")

    file_manager = FileManager()

    try:
        tree = file_manager.load_file(
            FileDirectory.MANUAL_EXPORT_PATH, Apple.XML_DATA, "xml"
        )
        root = tree.getroot()

        apple_df = extract_sleep_data(root, record_type)

        if not apple_df.empty:
            file_manager.save_file(FileDirectory.RAW_DATA_PATH, record_type, apple_df)
        else:
            logger.warning("No {record_type} data extracted. No file saved.")

    except Exception as e:
        logger.error(f"Error extracting Apple {record_type} data: {str(e)}")


if __name__ == "__main__":
    main()
