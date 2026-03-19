import SubscriptionDetailsClient from "./_components/subscription-details-client";

const SubscriptionDetailsPage = async ({
	searchParams,
}: {
	searchParams: Promise<{ filter?: string }>;
}) => {
	const { filter } = await searchParams;
	return <SubscriptionDetailsClient filter={filter ?? ""} />;
};

export default SubscriptionDetailsPage;
