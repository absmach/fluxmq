import { ArchitectureSection } from "@/components/homepage/section-architecture";
import { FeaturesSection } from "@/components/homepage/section-features";
import { FooterSection } from "@/components/homepage/section-footer";
import { HeroSection } from "@/components/homepage/section-hero";
import { NewsletterSection } from "@/components/homepage/section-newsletter";
import { PerformanceSection } from "@/components/homepage/section-performance";
import { QuickStartSection } from "@/components/homepage/section-quickstart";
import { UseCasesSection } from "@/components/homepage/section-usecases";

export default function HomePage() {
  return (
    <main className="min-h-screen bg-theme">
      <HeroSection />
      <FeaturesSection />
      <PerformanceSection />
      <UseCasesSection />
      <ArchitectureSection />
      <QuickStartSection />
      <NewsletterSection />
      <FooterSection />
    </main>
  );
}
