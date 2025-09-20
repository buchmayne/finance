from etl.orchestration import ETLPipeline

if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run_full_pipeline()
    print("Complete")