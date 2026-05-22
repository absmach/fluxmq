"use client";
import { useRouter } from "next/navigation";
import { useEffect } from "react";

export function RootRedirect() {
  const router = useRouter();
  useEffect(() => {
    router.replace("/getting-started");
  }, [router]);
  return null;
}
